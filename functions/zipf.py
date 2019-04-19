import random
import numpy as np 

def get_base(N, skew):
	base = 0.0
	for k in range(1, N+1):
		base += np.power(k, -1*skew)
	return 1 / float(base)


def sample(n, base, sum_probs):
	zipf_value = None
	low = 1
	high = n

	z = random.random()
	while z == 0 or z == 1:
		z = random.random()
	#print("z is %f" % z)

	while True:
		mid = int(np.floor((low + high) / 2))
		#print("mid is %d, low is %d, high is %d" % (mid, low, high))

		if sum_probs[mid] >= z and sum_probs[mid - 1] < z:
			zipf_value = mid
			break
		elif sum_probs[mid] >= z:
			high = mid - 1
		else:
			low = mid + 1
		if low > high:
			break
	return zipf_value

total_num_keys = 10000
zipf = 1.0
base = get_base(total_num_keys, zipf)
sum_probs = {}
sum_probs[0] = 0.0
for i in range(1, total_num_keys+1):
    sum_probs[i] = sum_probs[i - 1] + (base / np.power(float(i), zipf))

count = {}
i = 0
while (i < 10000):
	#print(i)
	k = sample(total_num_keys, base, sum_probs)
	if k not in count:
		count[k] = 1
	else:
		count[k] += 1
	i += 1

for k in count:
	if count[k] > 10:
		print("key is %s and count is %d" % (k, count[k]))