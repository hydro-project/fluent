from add_nodes import add_nodes
import util

c = util.init_k8s()

add_nodes(c, '../conf/kvs-base.yml', ['benchmark'], [2], ['100.96.1.6'], route_addr='a618b2308620311e980e70aa83f6a580-500771572.us-east-1.elb.amazonaws.com', function_addr='ac3457807620411e980e70aa83f6a580-2097223521.us-east-1.elb.amazonaws.com')
