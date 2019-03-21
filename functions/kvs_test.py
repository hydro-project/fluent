import sched_func
import cloudpickle as cp
from anna.client import *
from anna.lattices import *

r_elb = 'a31373ab54bec11e9a5a40a9f27cf828-1977051996.us-east-1.elb.amazonaws.com'
fbody = cp.dumps(sched_func.scheduler)
fbl = LWWPairLattice(0, fbody)

client = AnnaClient(r_elb, '3.89.81.130')
client.put('funcs/scheduler', fbl)
