#  Copyright 2018 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from anna.client import AnnaClient
from client import SkyReference
import cloudpickle as cp
import flask
from flask import session
from flask_session import Session
import logging
import os
from threading import Thread
import time
import uuid
import zmq

REPORT_THRESH = 30
logging.basicConfig(filename='log.txt',level=logging.INFO)

app = flask.Flask(__name__)
report_start = time.time()
ctx = zmq.Context(1)
global_util = 0.0

routing_addr = os.environ['ROUTE_ADDR']
mgmt_ip = os.environ['MGMT_IP']
ip = os.environ['MY_IP']

client = AnnaClient(routing_addr, ip)

@app.route('/create/<funcname>', methods=['POST'])
def create_func(funcname):
    func_binary = flask.request.get_data()

    app.logger.info('Creating function: ' + funcname + '.')
    client.put(funcname, func_binary)

    funcs = _get_func_list('')
    funcs.append('funcs/' + funcname)
    client.put('allfuncs', cp.dumps(funcs))

    return construct_response()

@app.route('/remove/<funcname>', methods=['POST'])
def remove_func(funcname):
    app.logger.info('Removing function: ' + funcname + '.')
    client.remove(funcname)

    return construct_response()

def _get_func_list(prefix, fullname=False):
    funcs = client.get('allfuncs')
    if len(funcs) == 0:
        return []
    funcs = cp.loads(funcs)

    result = []
    prefix = "funcs/" + prefix

    for f in funcs:
        if f.startswith(prefix):
            if fullname:
                result.append(f)
            else:
                result.append(f[6:])

    return result

@app.route('/<funcname>', methods=['POST'])
def call_func(funcname):
    app.logger.info('Calling function: ' + funcname + '.')
    obj_id = str(uuid.uuid4())
    t = Thread(target=_exec_func, args=(funcname, app.logger, obj_id, flask.request.get_data()))
    t.start()

    return construct_response(obj_id)

def _exec_func(funcname, logger, obj_id, arg_obj):
    start = time.time()
    func_binary = client.get(funcname)
    func = cp.loads(func_binary)

    args = cp.loads(arg_obj)

    func_args = ()
    flog = open('flog.txt', 'a+')

    for arg in args:
        if isinstance(arg, SkyReference):
            func_args = (_resolve_ref(arg, client),)
        else:
            func_args += (arg,)


    res = func(*func_args)
    flog.write('Putting result %s into KVS at id %s.\n' % (str(res), obj_id))

    client.put(obj_id, cp.dumps(res))
    end = time.time()
    global_util += (end - start)

    # periodically report function occupancy
    report_end = time.time()
    if report_end - report_start > REPORT_THRESH:
        util = (global_util * 1000) / REPORT_THRESH

        sckt = ctx.socket(zmq.PUSH)
        sckt.connect('tcp://' + mgmt_ip + ':7002')
        sckt.send_string(str(util))

        flog.write('Sending utilization of %.2f%%.' % (util))

        report_start = time.time()
        global_util = 0

    flog.close()


def _resolve_ref(ref, client):
    ref_data = client.get_object(ref.key)

    if ref.deserialize:
        return cp.loads(ref_data)
    else:
        return ref_data

@app.route('/list', methods=['GET'])
@app.route('/list/<prefix>', methods=['GET'])
def list_funcs(prefix=''):
    result = _get_func_list(prefix)

    return construct_response(result)

def construct_response(obj=None):
    resp = flask.make_response()
    if obj != None:
        resp.data = cp.dumps(obj)
        resp.content_type = 'text/plain'

    resp.status_code = 200

    return resp

def return_error(error=''):
    resp = flask.make_response()
    if error != '':
        resp.data = error
        resp.content_type = 'text/plain'

    resp.status_code = 400

    return resp

def run():
    app.secret_key = "this is a secret key"
    Session(app)
    app.run(threaded=True, host='0.0.0.0', port=7000)

run()