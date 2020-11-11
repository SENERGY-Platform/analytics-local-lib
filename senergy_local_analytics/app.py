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
import typing

import jsonpath_rw_ext as jp
import paho.mqtt.client as mqtt

from senergy_local_analytics import config_decoder, topic_decoder, Input, InputTopic, OutputMessage, Config, Output
from senergy_local_analytics.util import InternalJSONEncoder


class App:
    _inputs = [None]
    _process_message = None

    def __init__(self, config_path='config.json'):
        self._client = mqtt.Client()
        if os.getenv("CONFIG") is not None:
            self._config: Config = json.loads(os.getenv("CONFIG"), object_hook=config_decoder)
        else:
            with open(config_path) as json_file:
                data = json.load(json_file)
                self._config: Config = json.loads(json.dumps(data["config"]), object_hook=config_decoder)
        if os.getenv("INPUT") is not None:
            self._topics = json.loads(os.getenv("INPUT"), object_hook=topic_decoder)
        else:
            with open(config_path) as json_file:
                data = json.load(json_file)
                self._topics = json.loads(json.dumps(data["inputTopics"]), object_hook=topic_decoder)
        self._output_message = OutputMessage(self._config.pipeline_id, self._config.operator_id)
        self._client.on_connect = self.__on_connect
        self._client.on_message = self.__on_message

    def main(self) -> None:
        self._client.connect(os.getenv("BROKER_HOST", "localhost"), int(os.getenv("BROKER_PORT", 1883)), 60)
        self._client.loop_forever()

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
                            inp.add_input_topic(InputTopic(self.__check_topic_name(topic), source))
        self._inputs = inputs

    def process_message(self, func: typing.Callable[[typing.List[Input]], Output]) -> None:
        self._process_message = func

    def __on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc), flush=True)
        tops = []
        for topicConfig in self._topics:
            tops.append((self.__check_topic_name(topicConfig), 0))
            print(topicConfig, flush=True)
        client.subscribe(tops)

    def __on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        message = msg.payload.decode('utf8').replace('"{', '{').replace('}"', '}').replace('\\', '')
        for inp in self._inputs:
            return_topics = inp.get_input_topics_by_name(msg.topic)
            for topic in return_topics:
                inp.current_topic = topic.topic_name
                inp.current_source = topic.source
                inp.current_value = jp.match1("$." + topic.source, json.loads(message))
        self.__actually_process_message()

    def __actually_process_message(self):
        if callable(self._process_message):
            output = self._process_message(self._inputs)
            for output_name, value in output.values.items():
                self.__set_output(output_name, value)
            if output.send:
                self.__send_message()

    def __send_message(self):
        self._output_message.set_time_now()
        payload = self._output_message
        self._client.publish("fog/analytics/" + self._config.output_topic +
                             "/" + self._config.operator_id,
                             payload=json.dumps(payload, cls=InternalJSONEncoder), qos=0, retain=False)

    def __set_output(self, output_name, value):
        self._output_message.analytics[output_name] = value

    def __check_topic_name(self, topic_config) -> str:
        if topic_config.filter_type == "OperatorId":
            topic_name = "fog/analytics/" + topic_config.name + "/" + topic_config.filter_value
        else:
            topic_name = topic_config.name
        return topic_name
