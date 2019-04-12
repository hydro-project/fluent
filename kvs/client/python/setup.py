from distutils.core import setup
import os
from setuptools.command.install import install

class InstallWrapper(install):
    def run(self):
        # compile the relevant protobufs
        self.compile_proto()

        # Run the standard PyPi copy
        install.run(self)

        # remove the compiled protobufs
        self.cleanup()

    def compile_proto(self):
        # compile the protobufs
        os.system('cd anna && protoc -I=../../../../include/proto --python_out=. ' +
                'kvs.proto')

        os.system('cd anna && protoc -I=../../../../include/proto --python_out=. ' +
                'functions.proto')

    def cleanup(self):
        os.system('rm anna/kvs_pb2.py')

setup(
        name='Anna',
        version='0.1',
        packages=['anna', ],
        license='Apache v2',
        long_description='Client for the Anna KVS',
        install_requires=['zmq', 'protobuf'],
        cmdclass={'install': InstallWrapper}
)
