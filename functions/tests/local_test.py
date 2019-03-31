import sys
import time

def square(x):
    return x*x

def incr(x):
    return x + 1

count = int(sys.argv[1])

start = time.time()
for _ in range(count):
    res = square(incr(1))

end = time.time()

print('Total elapsed time: %.6f ms' % ((end - start) * 1000))
print('Average latency: %.6f ms' % ((end - start) / count * 1000))
