# Fluent

[![Build Status](https://travis-ci.com/fluent-project/fluent.svg?branch=master)](https://travis-ci.com/fluent-project/fluent)
[![codecov](https://codecov.io/gh/fluent-project/fluent/branch/master/graph/badge.svg)](https://codecov.io/gh/fluent-project/fluent)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


Fluent is a fully managed, data-first computation framework under development that [U.C. Berkeley](https://www.berkeley.edu) [RISE Lab](https://rise.cs.berkeley.edu). There are two main components in Fluent. 

The first is a key-value store based on prior open-source work from the RISE lab (fka Anna). The Fluent KVS is an elastic, cloud-native storage engine that uses coordination-avoiding techniques and asynchronous message passing to provide very low latency. You can find more information about running and using the Fluent KVS in the `kvs` directory.

The second component is a data-centric programming framework, built on top of the Anna KVS. The goal of the programming framework is to provide users a general-purpose API and runtime for executing programs on data stored in the Anna KVS. Users are able to submit arbitrary code or containers for execution, and we plan to support performance SLOs for function execution. 

## Getting Started

```bash
# install required dependencies; if you're on a Mac, please use install-dependencies-osx.sh
$ ./scripts/install-dependencies.sh
$ ./scripts/build-all.sh
$ ./scripts/start-kvs-local.sh n n
$ ./scripts/start-runtime-local.sh
```

To build only the KVS, please run `./scripts/build-kvs.sh`.

**TODO** Add more information about how to start the client and run code.

**TODO**: Add more information about starting and running in Kubernetes.

## Contributing

If you run into any issues, please open an issue and make sure to include relevant information (e.g., stack traces) as well as operating system, dependency versions, etc.

If you are looking to contribute to the project, please look at our [issues list](https://github.com/ucbrise/fluent/issues), particularly those marked as [good for beginners](https://github.com/ucbrise/fluent/issues?q=is%3Aopen+is%3Aissue+label%3Abeginners) and [help wanted](https://github.com/ucbrise/fluent/labels/help%20wanted).
