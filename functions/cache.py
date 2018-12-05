#  Copyright 2018 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

class LruCache:
    def __init__(self, num_entries=100):
        self.num_entries = num_entries

        self.cache = {}
        self.access_order = []

    def get(self, key):
        if key not in self.cache.keys():
            return None

        self.access_order.remove(key)
        self.access_order = [key, ] + self.access_order

        return self.cache[key]

    def put(self, key, val):
        self.cache[key] = val
        if key in self.access_order:
            self.access_order.remove(key)

        self.access_order = [key, ] + self.access_order

        if len(self.cache) > self.num_entries:
            last_key = self.access_order.pop()
            del self.cache[last_key]
