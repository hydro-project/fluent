import cloudpickle as cp
import logging
import numpy as np
import random
import sys
import time
import uuid

from include.shared import *

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def generate_dag(function_list, length):
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

def generate_arg_map(functions, connections, key_space):
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
            # sample key uniformly
            key = random.randint(0, key_space)
            key = str(key).zfill(len(str(key_space)) + 1)

            if key not in keys_chosen:
                keys_chosen.append(key)
                refs += (FluentReference(key, True, CROSSCAUSAL),)
                to_generate -= 1
                keys_read.append(key)

        arg_map[func] = refs
        
    return arg_map, set(keys_read)

func_list = ['f1', 'f2', 'f3', 'f4', 'f5']

functions, connections, length = generate_dag(func_list, 1)

logging.info("DAG contains %d functions" % len(functions))

for conn in connections:
	logging.info("(%s, %s)" % (conn[0], conn[1]))

logging.info("DAG length is %d" % length)

'''count = 0
num_func = 0

while count < 100000:
	result = generate_dag(func_list)
	num_func += len(result[0])
	count += 1

print("avg number of function is %f" % (float(num_func)/count))'''

#result = generate_dag(func_list)

#print("DAG contains %d functions" % len(result[0]))

#for conn in result[1]:
#	print("(%s, %s)" % (conn[0], conn[1]))

