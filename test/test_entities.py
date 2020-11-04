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

from senergy_local_analytics import Config, config_decoder, OutputMessage


class TestMainMethods(unittest.TestCase):

    def test_config_decoder(self):
        with open('./data/config-1.json') as json_file:
            data_in = json.load(json_file)
        config: Config = json.loads(json.dumps(data_in), object_hook=config_decoder)
        message = OutputMessage(config.pipeline_id, config.operator_id)
        self.assertEqual("d26eabc6-419b-4c98-965a-66dec914746a", message._operator_id)

    def test_output_message(self):
        message = OutputMessage("1", "1")
        message.set_time_now()
        print(message._time)
