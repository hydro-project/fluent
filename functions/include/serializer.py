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

import cloudpickle as cp
import codecs
from io import BytesIO
import numpy as np

from .functions_pb2 import *
from . import shared

SER_FORMAT = 'raw_unicode_escape'

class Serializer():
    def __init__(self):
        raise NotImplementedError('Cannot instantiate abstract class.')

    def _serialize(self, msg):
        pass

    def _deserialize(self, msg):
        pass

    def dump(self, msg):
        pass

    def load(self, msg):
        pass

class DefaultSerializer(Serializer):
    def __init__(self):
        pass

    def _serialize(msg):
        return msg

    def _deserialize(self, msg):
        return msg

    def dump(self, msg):
        return cp.dumps(msg)

    def load(self, msg):
        return cp.loads(msg)

class StringSerializer(Serializer):
    def __init__(self):
        pass

    def _serialize(self, msg):
        return codecs.decode(msg, SER_FORMAT)

    def _deserialize(self, msg):
        return codecs.encode(msg, SER_FORMAT)

    def dump(self, msg):
        return self._serialize(cp.dumps(msg))

    def load(self, msg):
        return cp.loads(self._deserialize(msg))

# TODO: how can we make serializers pluggable?
class NumpySerializer(DefaultSerializer):
    def __init__(self):
        pass

    def dump(self, msg):
        body = BytesIO()

        np.save(body, msg)
        return body.getvalue()

    def load(self, msg):
        return np.load(BytesIO(msg))

numpy_ser = NumpySerializer()
default_ser = DefaultSerializer()
string_ser = StringSerializer()

function_ser = default_ser

def get_serializer(kind):
    global numpy_ser, default_ser, string_ser

    if kind == NUMPY:
        return numpy_ser
    elif kind == STRING:
        return string_ser
    elif kind == DEFAULT:
        return default_ser
    else:
        return default_ser

def serialize_val(val, valobj=None, serialize=True):
    if not valobj:
        valobj = Value()

    if isinstance(val, shared.FluentFuture):
        valobj.body = default_ser.dump(shared.FluentReference(val.obj_id,
            True, LWW))
    elif isinstance(val, np.ndarray):
        valobj.body = numpy_ser.dump(val)
        valobj.type = NUMPY
    else:
        valobj.body = default_ser.dump(val)

    if not serialize:
        return valobj

    return valobj.SerializeToString()

def deserialize_val(val):
    v = Value()
    v.ParseFromString(val)

    if v.type == DEFAULT:
        return default_ser.load(v.body)
    elif v.type == STRING:
        return string_ser.load(v.body)
    elif v.type == NUMPY:
        return numpy_ser.load(v.body)
