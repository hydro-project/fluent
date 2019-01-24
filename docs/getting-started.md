# Getting Started

You can install the KVS and function serving environment locally either to play around with the system or for development. If you would like to start a cluster, please see the [Getting Started on AWS](getting-started-aws.md) docs.

```bash
# install required depenencies
$ ./scripts/install-dependencies.sh # please use install-dependencies-osx.sh if you are on a Mac

# compile and build all C++ components
$ ./scripts/build-kvs.sh -j4 -bRelease # to build with tests, change -bRelease to -bDebug -t

# start the KVS locally
$ ./scripts/start-kvs-local.sh n n

# start the FaaS server 
$ cd functions && python3 function_server.py 

```

Once you have the FaaS server running, you can start the client in a separate Python shell:

```python3
>>> import client
>>> cloud = FluentConnnection('127.0.0.1', '127.0.0.1')
>>> cloud.list()
sum
square
>>> sum = cloud.get('sum')
>>> sum(2, 2).get() # make a remote function call
4
```

You can see the [function API](function-api.md) docs for more on how to use the function server client. 