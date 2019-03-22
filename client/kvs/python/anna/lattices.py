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

class Lattice:
    def __init__(self):
        raise NotImplementedError

    def reveal(self):
        raise NotImplementedError

    def assign(self, value):
        raise NotImplementedError

    def merge(self, other):
        raise NotImplementedError

class LWWPairLattice(Lattice):
    def __init__(self, timestamp, value):
        if type(timestamp) != int or type(value) != bytes:
            raise ValueError('LWWPairLattice must be a bytes-value pair.')

        self.ts = timestamp
        self.val = value

    def reveal(self):
        return (self.ts, self.val)

    def assign(self, value):
        if type(value) == str:
            value = bytes(values, 'utf-8')

        if type(value) != tuple or type(value[0]) != int \
                or type(value[1]) != bytes:
            raise ValueError('LWWPairLattice must be a bytes-value pair.')

        self.ts = value[0]
        self.val = value[1]

    def merge(self, other):
        if other.ts > self.ts:
            return other
        else:
            return self

class SetLattice(Lattice):
    def __init__(self, value={}):
        if type(value) != set:
            raise ValueError('SetLattice can only be formed from a set.')

        self.val = value

    def reveal(self):
        return self.val

    def assign(self, value):
        if type(value) != set:
            raise ValueError('SetLattice can only be formed from a set.')

        self.val = value

    def merge(self, other):
        if type(other) != SetLattice:
            raise ValueError('Cannot merge SetLattice with invalid type ' +
                    str(type(other)) + '.')

        new_set = {}

        for v in other.val:
            new_set.insert(v)

        for v in self.val:
            new_set.insert(v)

        return SetLattice(new_set)
