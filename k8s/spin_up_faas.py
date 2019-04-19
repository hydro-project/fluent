from add_nodes import add_nodes
import util

c = util.init_k8s()

add_nodes(c, '../conf/kvs-base.yml', ['scheduler'], [2], ['100.96.1.6'], route_addr='a618b2308620311e980e70aa83f6a580-500771572.us-east-1.elb.amazonaws.com')
add_nodes(c, '../conf/kvs-base.yml', ['function'], [3], ['100.96.1.6'], route_addr='a618b2308620311e980e70aa83f6a580-500771572.us-east-1.elb.amazonaws.com', scheduler_ips=['172.20.49.188', '172.20.33.135'])