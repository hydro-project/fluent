from anna.client import AnnaClient
from anna.lattices import *
import cloudpickle as cp
from include.serializer import *
from include.shared import *
import numpy as np
import uuid

kvs = AnnaClient('ab5ebd57a5c7711e9a0940a7fd133056-1728542248.us-east-1.elb.amazonaws.com', '18.232.50.191')

NUM_OBJECTS = 200

oids = []

for _ in range(NUM_OBJECTS):
    array = np.random.rand(1024 * 1024)
    oid = str(uuid.uuid4())
    val = LWWPairLattice(0, serialize_val(array))

    kvs.put(oid, val)
    oids.append(oid)

oid_data = cp.dumps(oids)
l = LWWPairLattice(generate_timestamp(0), oid_data)
kvs.put('LOCALITY_OIDS', l)
