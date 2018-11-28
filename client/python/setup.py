from distutils.core import setup

setup(
        name='Anna',
        version='0.1',
        packages=['anna', ],
        license='Apache v2',
        long_description='Client for the Anna KVS',
        install_requires=['zmq', 'protobuf']
)
