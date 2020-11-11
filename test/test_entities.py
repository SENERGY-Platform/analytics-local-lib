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
import unittest
from senergy_local_analytics import Config, config_decoder, OutputMessage, App, Input, Output, InputTopic, topic_decoder


class TestMainMethods(unittest.TestCase):

    def test_config_decoder(self):
        with open('./data/config-1.json') as json_file:
            data_in = json.load(json_file)
        config: Config = json.loads(json.dumps(data_in), object_hook=config_decoder)
        message = OutputMessage(config.pipeline_id, config.operator_id)
        self.assertEqual("d26eabc6-419b-4c98-965a-66dec914746a", message._operator_id)

    def test_topic_decoder(self):
        with open('./data/config-3.json') as json_file:
            data_in = json.load(json_file)
        topic: InputTopic = json.loads(json.dumps(data_in), object_hook=topic_decoder)
        self.assertRaises(AttributeError, getattr, topic, "topic_name")

    def test_output_message_set_time(self):
        message = OutputMessage("1", "1")
        message.set_time_now()
        self.assertTrue(len(message._time) > 0)

    def test_set_output(self):
        app = App('./data/config-2.json')
        app.process_message(self.__process)
        app._App__actually_process_message()
        self.assertEqual({'sum': 3}, app._output_message.analytics)

    def test_config(self):
        app = App('./data/config-4.json')
        input1 = Input("value")
        input2 = Input("timestamp")
        app.config([input1, input2])

    def __process(self, inputs: list[Input]):
        return Output(False, {"sum": 3})
