import boto3
import json
import sys
import time

lambd = boto3.client('lambda')
s3 = boto3.client('s3')

count = int(sys.argv[1])

fail_count = 0
start = time.time()
for _ in range(count):
    ret = lambd.invoke(FunctionName='incr', Payload=json.dumps({'arg': 1}))
    ptr = json.loads(ret['Payload'].read())

    res = json.loads(s3.get_object(Bucket='vsreekanti', Key=ptr)['Body'].read())['result']

    if res != 4:
        fail_count += 1

end = time.time()

print('Total elapsed time: %.2f' % (end - start))
print('Average latency: %.4f' % ((end - start) / count))
print('Total number of failed requests: %d' % (fail_count))
