#!/usr/bin/env python3

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

import os
import subprocess
import yaml

NAMESPACE = 'default'
EBS_VOL_COUNT = 4

def replace_yaml_val(yamlobj, name, val):
    for pair in yamlobj:
        if pair['name'] == name:
            pair['value'] = val
            return

def load_yaml(filename):
    try:a
        with open(filename, 'r') as f:
            return yaml.load(f.read())
    except Error as e:
        print('Unexpected error while loading YAML file:')
        print(e.stderr)
        print('')
        print('Make sure to clean up the cluster object and state store \
                before recreating the cluster.')
        sys.exit(1)

def run_process(command):
    try:
        subprocess.run(command, cwd='./kops', check=True)
    except CalledProcessError as e:
        print('Unexpected error while running command %s:' % (e.cmd))
        print(e.stderr)
        print('')
        print('Make sure to clean up the cluster object and state store \
                before recreating the cluster.')
        sys.exit(1)

def check_or_get_env_arg(argname):
    if argname not in os.environ:
        print('Required argument %s not found as an environment variable. \
                Please specify before re-running.' % (argname))
        sys.exit(1)

    return os.environ[argname]
