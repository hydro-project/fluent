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
            raise ValueError('LWWPairLattice must be a timestamp-bytes pair.')

        self.ts = timestamp
        self.val = value

    def reveal(self):
        return (self.ts, self.val)

    def assign(self, value):
        if type(value) == str:
            value = bytes(values, 'utf-8')

        if type(value) != tuple or type(value[0]) != int \
                or type(value[1]) != bytes:
            raise ValueError('LWWPairLattice must be a timestamp-bytes pair.')

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

# A wrapper class that implements some convenience OrderedSet operations on top of a list.
# We use this because it is way cheaper to deserialize into,
# at the cost of having expensive reordering operations (e.g. random insert),
# which we expect to be rare for our use cases (we will almost always be inserting at the end).
class ListBasedOrderedSet:
    # Preconditions: iterable's elements are unique and sorted ascending.
    # Behaviour is undefined if it is not.
    def __init__(self, iterable=[]):
        self.lst = []
        for val in iterable:
            self.insert(val)

    # Inserts a value, maintaining sorted order.
    def insert(self, value):
        # Microoptimization for the common case.
        if len(self.lst) == 0:
            self.lst.append(value)
        elif value > self.lst[-1]:
            self.lst.append(value)
        else:
            idx, present = self._index_of(value)
            if not present:
                self.lst.insert(idx, value)

    # Finds the index of an element, or where to insert it if you want to maintain sorted order.
    # Returns (int index, bool present).
    # E.g. _index_of(lst, 'my-value') -> (42, true)
    #           => lst[42] = 'my-value'
    #      _index_of(lst, 'my-value') -> (42, false)
    #           => lst[41] < 'my-value' < lst[42]
    def _index_of(self, value):
        low = 0
        high = len(self.lst)
        while low < high:
            middle = low + int((high - low) / 2)
            pivot = self.lst[middle]
            if value == pivot:
                return (middle, True)
            elif value < pivot:
                high = middle
            elif pivot < value:
                low = middle + 1
        return (low, False)


class OrderedSetLattice(Lattice):
    def __init__(self, value = ListBasedOrderedSet()):
        if type(value) != ListBasedOrderedSet:
            raise ValueError("OrderedSetLattice can only be formed from a ListBasedOrderedSet for now.")
        self.val = value

    def reveal(self):
        return self.val

    def assign(self, value):
        if type(value) != ListBasedOrderedSet:
            raise ValueError("OrderedSetLattice can only be formed from a ListBasedOrderedSet for now.")
        self.val = value

    def merge(self, other):
        if type(other) != OrderedSetLattice:
            raise ValueError("Cannot merge OrderedSetLattice with type " +
                str(type(other)) + ".")

        # Merge the two sorted lists by lockstep merge.
        # Note that reconstruction is faster than in-place merge.
        new_lst = []

        other = other.reveal().lst
        us = self.val.lst
        i, j = 0, 0  # Earliest unmerged indices.
        while i < len(us) or j < len(other):
            if i == len(us):
                new_lst.extend(other[j:])
                break
            elif j == len(other):
                new_lst.extend(us[i:])
                break
            else:
                a = us[i]
                b = other[j]
                if a == b:
                    new_lst.append(a)
                    i += 1
                    j += 1
                elif a < b:
                    new_lst.append(a)
                    i += 1
                elif b < a:
                    new_lst.append(b)
                    j += 1

        return OrderedSetLattice(ListBasedOrderedSet(new_lst))
