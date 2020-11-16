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
from threading import Thread

from senergy_local_analytics import Message


class Worker(Thread):

    def __init__(self, queue, target=None):
        Thread.__init__(self)
        self.queue = queue
        self.target = target

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            message: Message = self.queue.get()
            try:
                self.target(message)
            finally:
                self.queue.task_done()
