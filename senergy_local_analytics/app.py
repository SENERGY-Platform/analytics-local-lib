#  Copyright 2020 InfAI (CC SES)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import json
import os
import queue
import typing
import uuid
from concurrent.futures.thread import ThreadPoolExecutor

import paho.mqtt.client as mqtt

from senergy_local_analytics import operator_config_decoder, topic_decoder, Input, InputTopic, OutputMessage, Config, \
    Output, \
    Message, OperatorConfig
from senergy_local_analytics.util import InternalJSONEncoder


class App:
    _inputs = [None]
    _process_message = None

    def __init__(self, config_path='config.json'):
        self.__msg_queue = queue.Queue()
        self._client = mqtt.Client(client_id=str(uuid.uuid4()))
        self._operator_config: OperatorConfig
        self._topics = None
        self._config: Config = Config()
        self.__load_configs(config_path)
        if self._operator_config is None:
            print("No valid config found")
            exit(1)
        self._output_message = OutputMessage(self._operator_config.pipeline_id, self._operator_config.operator_id)
        self._client.on_connect = self.__on_connect
        self._client.on_message = self.__on_message

    def __load_configs(self, config_path='config.json'):
        try:
            with open(config_path) as json_file:
                data = json.load(json_file)
                if "config" in data:
                    self._operator_config = json.loads(json.dumps(data["config"]), object_hook=operator_config_decoder)
                if "inputTopics" in data:
                    self._topics = json.loads(json.dumps(data["inputTopics"]), object_hook=topic_decoder)
                if "operatorConfig" in data:
                    self._config.set_fields(json.loads(json.dumps(data["operatorConfig"])))
        except FileNotFoundError as err:
            print("Config File not found:" + err.filename)
        
        config = os.getenv("CONFIG")
        if config is not None:
            print("Config: " + config)
            self._operator_config = json.loads(config, object_hook=operator_config_decoder)
        
        inputConf = os.getenv("INPUT")
        if inputConf is not None:
            print("Input: " + inputConf)
            self._topics = json.loads(inputConf, object_hook=topic_decoder)
        
        operator_config = os.getenv("OPERATOR_CONFIG")
        if operator_config is not None:
            print("Operator Config: " + operator_config)
            self._config.set_fields(json.loads(operator_config))

    def main(self) -> None:
        self._client.connect(os.getenv("BROKER_HOST", "localhost"), int(os.getenv("BROKER_PORT", 1883)), 60)
        self._client.loop_start()
        with ThreadPoolExecutor(max_workers=10) as executor:
            while True:
                message: Message = self.__msg_queue.get()
                future = executor.submit(self.__parse_and_process_message, message)
                future.result()

    def __parse_and_process_message(self, message: Message):
        self.__get_input_values(message.get_message(), message.get_topic())
        self.__actually_process_message()

    def config(self, inputs: typing.List[Input]) -> None:
        for topic in self._topics:
            if hasattr(topic, 'name'):
                for mapping in topic.mappings:
                    source = "no_source_given"
                    if hasattr(mapping, 'source'):
                        if topic.filter_type == "OperatorId":
                            source = "analytics." + mapping.source
                        else:
                            source = mapping.source
                    for inp in inputs:
                        if inp.name == mapping.dest:
                            inp.add_input_topic(InputTopic(topic.name, source))
        self._inputs = inputs

    def process_message(self, func: typing.Union[typing.Callable[[typing.List[Input], Config], Output],
                                                 typing.Callable[[typing.List[Input]], Output]]) -> None:
        if callable(func):
            self._process_message = func

    def __on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc), flush=True)
        client.subscribe(self.__create_topic_subscription_list())

    def __create_topic_subscription_list(self):
        tops = []
        for topic_config in self._topics:
            if hasattr(topic_config, 'name'):
                tops.append((topic_config.name, 0))
                print(topic_config, flush=True)
        return tops

    def __on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        self.__msg_queue.put_nowait(Message(msg.topic, msg.payload))

    def __get_input_values(self, message, topic_name: str):
        for inp in self._inputs:
            return_topics = inp.get_input_topics_by_name(topic_name)
            for topic in return_topics:
                inp.current_topic = topic.topic_name
                inp.current_source = topic.source
                val = json.loads(message)
                for v in topic.source_array:
                    val = val[v]
                inp.current_value = val

    def __actually_process_message(self):
        try:
            output = self._process_message(self._inputs, self._config)
        except TypeError:
            output = self._process_message(self._inputs)
        for output_name, value in output.values.items():
            self._output_message.set_output(output_name, value)
        if output.send:
            self.__send_message()

    def __send_message(self):
        self._output_message.set_time_now()
        payload = self._output_message
        self._client.publish(self._operator_config.output_topic, payload=json.dumps(payload, cls=InternalJSONEncoder), qos=0, retain=False)
