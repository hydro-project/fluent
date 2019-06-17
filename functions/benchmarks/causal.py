import cloudpickle as cp
import logging
import numpy as np
import random
import sys
import time
import uuid

from anna.lattices import *
from include.functions_pb2 import *
from include.kvs_pb2 import *
from include.serializer import *
from include.shared import *
from . import utils

def run(mode, segment, flconn, kvs, dags, dag_names):
    latency = {}
    if mode == 'create':
        ### DEFINE AND REGISTER FUNCTIONS ###
        def strmnp1(a,b):
            result = ''
            for i, char in enumerate(a):
                if i % 2 == 0:
                    result += a[i]
                else:
                    result += b[i]
            return result

        def strmnp2(a,b):
            result = ''
            for i, char in enumerate(a):
                if i % 2 == 0:
                    result += a[i]
                else:
                    result += b[i]
            return result

        def strmnp3(a,b):
            result = ''
            for i, char in enumerate(a):
                if i % 2 == 0:
                    result += a[i]
                else:
                    result += b[i]
            return result

        def strmnp4(a,b):
            result = ''
            for i, char in enumerate(a):
                if i % 2 == 0:
                    result += a[i]
                else:
                    result += b[i]
            return result

        def strmnp5(a,b):
            result = ''
            for i, char in enumerate(a):
                if i % 2 == 0:
                    result += a[i]
                else:
                    result += b[i]
            return result

        cloud_strmnp1 = flconn.register(strmnp1, 'strmnp1')
        cloud_strmnp2 = flconn.register(strmnp2, 'strmnp2')
        cloud_strmnp3 = flconn.register(strmnp3, 'strmnp3')
        cloud_strmnp4 = flconn.register(strmnp3, 'strmnp4')
        cloud_strmnp5 = flconn.register(strmnp3, 'strmnp5')

        if cloud_strmnp1 and cloud_strmnp2 and cloud_strmnp3 and cloud_strmnp4 and cloud_strmnp5:
            logging.info('Successfully registered the string manipulation functions.')
        else:
            sys.exit(1)

        ### TEST REGISTERED FUNCTIONS ###
        refs = ()
        for _ in range(2):
            val = '00000'
            ccv = CrossCausalValue()
            ccv.vector_clock['base'] = 1
            ccv.values.extend([serialize_val(val)])
            k = str(uuid.uuid4())
            logging.info("key name is %s" % k)
            succeed = kvs.put(k, ccv)
            logging.info("succeed is %s" % succeed)

            refs += (FluentReference(k, True, CROSSCAUSAL),)

        strmnp_test1 = cloud_strmnp1(*refs).get()
        logging.info('Successfully tested strmnp_test1.')
        strmnp_test2 = cloud_strmnp2(*refs).get()
        logging.info('Successfully tested strmnp_test2.')
        strmnp_test3 = cloud_strmnp3(*refs).get()
        logging.info('Successfully tested strmnp_test3.')
        strmnp_test4 = cloud_strmnp4(*refs).get()
        logging.info('Successfully tested strmnp_test4.')
        strmnp_test5 = cloud_strmnp5(*refs).get()
        logging.info('Successfully tested strmnp_test5.')

        if strmnp_test1 != '00000' or strmnp_test2 != '00000' or strmnp_test3 != '00000' or strmnp_test4 != '00000' or strmnp_test5 != '00000':
            logging.error('Unexpected result from strmnp(v1, v2): %s %s %s %s %s' % (str(strmnp_test1), str(strmnp_test2), str(strmnp_test3), str(strmnp_test4), str(strmnp_test5)))
            sys.exit(1)

        logging.info('Successfully tested functions!')

    elif mode == 'warmup':
        ### POPULATE DATA###
        val = '00000'
        ccv = CrossCausalValue()
        ccv.vector_clock['base'] = 1
        ccv.values.extend([serialize_val(val)])

        total_num_keys = 1000000
        bin_size = int(total_num_keys / 8)

        start = time.time()

        for i in range(segment*bin_size + 1, (segment + 1)*bin_size + 1):
            k = str(i).zfill(len(str(total_num_keys)))
            #ccv = CrossCausalValue()
            #ccv.vector_clock['base'] = 1
            #ccv.values.extend([serialize_val(k)])

            if i % 1000 == 0:
                logging.info('key is %s' % k)
            kvs.put(k, ccv)

        end = time.time()
        latency['warmup'] = end - start

        logging.info('Data populated')

        ### CREATE DAG ###
        # create 100 dags
        
        dag_num = 240
        bin_size = int(dag_num / 8)

        for dag_id in range(segment*bin_size + 1, (segment + 1)*bin_size + 1):
            dag_name = 'dag_' + str(dag_id)
            dag_names.append(dag_name)
            func_list = ['strmnp1', 'strmnp2', 'strmnp3', 'strmnp4', 'strmnp5']

            functions, connections, length = generate_dag_by_length(func_list, 1 + (dag_id % 4))

            success, error = flconn.register_dag(dag_name, functions, connections)

            if not success:
                logging.info('Failed to register DAG: %s' % (ErrorType.Name(error)))
                sys.exit(1)
            logging.info("Successfully created the DAG")

            logging.info("DAG contains %d functions" % len(functions))

            for conn in connections:
                logging.info("(%s, %s)" % (conn[0], conn[1]))

            dags[dag_name] = (functions, connections, length)

    elif mode == 'run':

        latency[1] = []
        latency[2] = []
        latency[3] = []
        latency[4] = []

        total_num_keys = 1000000


        ### CREATE ZIPF TABLE###
        zipf = 1.0
        base = get_base(total_num_keys, zipf)
        sum_probs = {}
        sum_probs[0] = 0.0
        for i in range(1, total_num_keys+1):
            sum_probs[i] = sum_probs[i - 1] + (base / np.power(float(i), zipf))

        logging.info("Created Probability Table with zipf %f" % zipf)

        ### RUN DAG ###
        max_vc_length = 0;
        client_num = 4000
        bin_size = int(client_num / 8)

        for i in range(segment*bin_size + 1, (segment + 1)*bin_size + 1):
            cid = 'client_' + str(i)
            if i % 100 == 0:
                logging.info("running client %s" % cid)

            # randomly pick a dag
            dag_name = random.choice(dag_names)
            functions, connections, length = dags[dag_name]

            arg_map, read_set = generate_arg_map(functions, connections, total_num_keys, base, sum_probs)

            #for func in arg_map:
            #    logging.info("function is %s" % func)
            #    for ref in arg_map[func]:
            #        logging.info("key of reference is %s" % ref.key)

            #for key in read_set:
            #    logging.info("read set contains %s" % key)

            output = random.choice(read_set)
            #output = 'result'

            start = time.time()
            rid = flconn.call_dag(dag_name, arg_map, consistency=CROSS, output_key=output, client_id=cid)
            #logging.info("Output key is %s" % rid)

            res = kvs.get(rid)
            while not res:
                #logging.info("key %s does not exist" % rid)
                res = kvs.get(rid)
            while cid not in res.vector_clock:
                #logging.info("client id %s not in key %s VC" % (cid, rid))
                #for k in res.vector_clock:
                #    logging.info("has %s" % k)
                res = kvs.get(rid)
            end = time.time()

            latency[length].append(end - start)

            #logging.info("size of vector clock is %d" % len(res.vector_clock))
            if len(res.vector_clock) > max_vc_length:
                max_vc_length = len(res.vector_clock)
            res = deserialize_val(res.values[0])

            if not res == '00000':
                logging.info("error, res is %s" % res)

        logging.info("max vector clock length is %d" % max_vc_length)

    return latency


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

    while True:
        mid = int(np.floor((low + high) / 2))
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

def generate_dag_by_length(function_list, length):
    available_functions = function_list.copy()
    functions = []
    connections = []


    end_func = random.choice(available_functions)
    functions.append(end_func)
    available_functions.remove(end_func)

    current_length = 1

    sink = end_func

    while not current_length == length:
        # pick a function
        source = random.choice(available_functions)
        functions.append(source)
        available_functions.remove(source)
        # populate connection
        connections.append((source, sink))
        current_length += 1
        sink = source

    return (functions, connections, length)

def generate_dag(function_list):
    available_functions = function_list.copy()
    functions = []
    connections = []

    to_generate = []

    end_func = random.choice(available_functions)
    functions.append(end_func)
    to_generate.append(end_func)
    available_functions.remove(end_func)

    while not len(to_generate) == 0:
        sink = to_generate.pop()
        for _ in range(2):
            if random.random() <= 0.4 and len(available_functions) > 0:
                # pick a function
                source = random.choice(available_functions)
                functions.append(source)
                to_generate.append(source)
                available_functions.remove(source)
                # populate connection
                connections.append((source, sink))

    length = {}
    for conn in connections:
        func_source = conn[0]
        length[func_source] = 1
        sink = conn[1]
        has_conn = True
        while has_conn:
            has_conn = False
            for conn in connections:
                if sink == conn[0]:
                    has_conn = True
                    length[func_source] += 1
                    sink = conn[1]

    max_length = 1
    for f in length:
        if length[f] > max_length:
            max_length = length[f]

    return (functions, connections, max_length)

def generate_arg_map(functions, connections, num_keys, base, sum_probs):
    arg_map = {}
    keys_read = []

    for func in functions:
        num_parents = 0 
        for conn in connections:
            if conn[1] == func:
                num_parents += 1

        to_generate = 2 - num_parents
        refs = ()
        keys_chosen = []
        while not to_generate == 0:
            # sample key from zipf
            key = sample(num_keys, base, sum_probs)
            key = str(key).zfill(len(str(num_keys)))

            if key not in keys_chosen:
                keys_chosen.append(key)
                refs += (FluentReference(key, True, CROSSCAUSAL),)
                to_generate -= 1
                keys_read.append(key)

        arg_map[func] = refs
        
    return arg_map, list(set(keys_read))
