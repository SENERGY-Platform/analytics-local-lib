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
import datetime
from collections import namedtuple


class Config:
    def __init__(self, pipeline_id, output_topic, operator_id):
        self.pipeline_id, self.output_topic, self.operator_id = pipeline_id, output_topic, operator_id


class Topic:
    def __init__(self, name, filter_type, filter_value, mappings):
        self.name, self.filter_type, self.filter_value, self.mappings = name, filter_type, filter_value, mappings


class Mapping:
    def __init__(self, source, dest):
        self.source, self.dest = source, dest


class InputTopic:
    def __init__(self, topic_name: str, source: str):
        self.topic_name, self.source = topic_name, source


class Input:
    def __init__(self, name):
        self.current_value = None
        self.current_topic = None
        self.current_source = None
        self.name = name
        self._input_topics: list[InputTopic] = []

    def add_input_topic(self, input_topic: InputTopic):
        self._input_topics.append(input_topic)

    def get_input_topics(self):
        return self._input_topics

    def get_input_topics_by_name(self, topic_name: str) -> list[InputTopic]:
        return_topics: list[InputTopic] = []
        for topic in self._input_topics:
            if topic.topic_name == topic_name:
                return_topics.append(topic)
        return return_topics

    def get_input_value_no(self) -> int:
        return len(self._input_topics)


class OutputMessage:
    _pipeline_id = None
    _operator_id = None
    _time = None

    def __init__(self, pipeline_id, operator_id):
        self._pipeline_id, self._operator_id = pipeline_id, operator_id
        self.analytics = {}

    def set_time_now(self):
        self._time = datetime.datetime.utcnow().isoformat()


def config_decoder(configDict):
    for key in configDict.keys():
        if key == "pipelineId":
            configDict["pipeline_id"] = configDict.pop("pipelineId")
        if key == "operatorId":
            configDict["operator_id"] = configDict.pop("operatorId")
        if key == "outputTopic":
            configDict["output_topic"] = configDict.pop("outputTopic")
    return namedtuple('X', configDict.keys())(*configDict.values())


def topic_decoder(topicDict):
    for key in topicDict.keys():
        if key == "filterType":
            topicDict["filter_type"] = topicDict.pop("filterType")
        if key == "filterValue":
            topicDict["filter_value"] = topicDict.pop("filterValue")
    return namedtuple('X', topicDict.keys())(*topicDict.values())