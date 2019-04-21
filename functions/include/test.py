from functions_pb2 import *

a = CausalRequest()
b = CausalRequest()
b.future_read_set.extend(['1','2'])
a.future_read_set.extend(b.future_read_set)

for k in a.future_read_set:
	print(k)