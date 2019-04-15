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

import pickle
import time
import uuid

from anna.lattices import *

class FluentRedisShim:
    def __init__(self, fluent_user_library):
        self._fluent_lib = fluent_user_library

    # Checking for existence of arbitrary keys.
    def exists(self, key):
        vals = self._fluent_lib.get(key)
        return vals[key] is not None

    ## Single value storage.

    # Retrieving arbitrary values that were stored by set().
    def get(self, key):
        lats = self._fluent_lib.get(key)
        lat = vals[key]
        value = lat.reveal()
        value = pickle.loads(value)
        return value

    # Storing arbitary values that can be retrieved by get().
    def set(self, key, value):
        ts = int(time.time())  # this is seconds; is that okay, or do we want milliseconds?
        value = pickle.dumps(value)
        lat = LWWPairLattice(timestamp=ts, value=value)
        self._fluent_lib.set(key, lat)

    ## Counter storage.

    # incr in Redis is used for two things:
    # - returning unique IDs
    # - as a counter
    # This mocks out the former.
    def incr(self, key):
        return uuid.uuid4()

    ## Set storage.
    # TODO everything; figure out how tombstones are gonna work.

    # Add an item to the set at this key.
    def sadd(self, key, value):
        value = pickle.dumps(value)
        lat = SetLattice({value,})
        self._fluent_lib.set(key, lat)

    # Remove an item from the set at this key.
    def srem(self, key, value):
        pass  # No removals in experiments rn; implement tombstones later if you need it.

    # Set contents.
    def smembers(self, key):
        lat = self._fluent_lib.get(key)
        assert type(lat) == SetLattice
        return lat.reveal()

    # Set membership.
    def sismember(self, key):
        lat = self._fluent_lib.get(key)
        assert type(lat) == SetLattice
        return key in lat.reveal()

    # Set size.
    def scard(self, key):
        lat = self._fluent_lib.get(key)
        assert type(lat) == SetLattice
        return len(lat.reveal())


    ## Append-only lists.

    # Append.
    def lpush(self, key, value):
        # microseconds.
        # This value will be 16 digits long for the foreseeable future.
        ts = int(time.time() * 1000000)
        value = pickle.dumps(value).decode()
        value = ('{}:{}'.format(ts, value)).encode()
        oset = ListBasedOrderedSet([value])
        lat = OrderedSetLattice(oset)
        self._fluent_lib.set(key, lat)

    # Slice.
    def lrange(self, key, begin, end):
        lat = self._fluent_lib.get(key)
        assert type(lat) == OrderedSetLattice
        oset = lat.reveal()
        values = [
            pickle.loads(item[17:])  # trim off timestamp + delimiter, and deserialize the rest.
            for item in oset.lst[begin:end]
        ]
        return values

    # Size.
    def llen(self, key):
        lat = self._fluent_lib.get(key)
        assert type(lat) == OrderedSetLattice
        return len(lat.reveal().lst)













