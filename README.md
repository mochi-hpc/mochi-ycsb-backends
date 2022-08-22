# YCSB C++ Interface

[YCSB](https://github.com/brianfrankcooper/YCSB) is one of the most popular Cloud
storage benchmark. However it is written in Java, forcing databases implemented
in other languages to provide a Java wrapper. While [YCSC-cpp](https://github.com/ls4154/YCSB-cpp)
provides a reimplementation of YCSB in C++, to date it only supports three backends, as
opposed to 45 for the original YCSB.

[ycsb-cpp-interface](https://github.com/mochi-hpc/ycsb-cpp-interface)
takes a different approach from YCSB-cpp, providing a Java/C++ library
that enables the use of C++ to write DB backends for YCSB.

ycsb-cpp-inteface works in a modular way, dynamically loading your C++ database
implementation from a library using a factory pattern.

## Installing

### Building manually

To build this repository from source, you will first need to have
its dependencies installed and findable by CMake. These dependencies
include:
- Java Development Kit (e.g., OpenJDK)
- YCSB
- cmake

Make sure to set the `JAVA_HOME` environment variable
to point to where your JDK is installed so that CMake can find it.
It is recommended to install a distribution of YCSB, rather than
the source.

You can then build the source contained in this repository as follows.

```
$ mkdir build
$ cd build
$ cmake .. -DYCSB_ROOT=<path/to/where/ycsb/is/installed> \
           -DCMAKE_INSTALL_PREFIX=<install/prefix>
$ make
```

### Installing using Spack

You can install this library using [Spack](https://spack.io/).
The `ycsb-cpp-interface` Spack package is available via the
[Mochi repository](https://github.com/mochi-hpc/mochi-spack-packages),
which can be added to Spack as follows.

```
$ git clone https://github.com/mochi-hpc/mochi-spack-packages.git
$ spack repo add mochi-spack-packages
```

Once the `mochi-spack-packages` repository has been made available to Spack,
you can install `ycsb-cpp-interface` as follows.

```
$ spack install ycsb-cpp-interface
```

## Testing

If you have installed ycsb-cpp-interface with Spack, make sure that
the package is loaded (`spack load ycsb-cpp-interface`), then you
can start the CLI for testing, as follows.

```
ycsb-cpp-cli
```

When building from source, the CLI is located in the `bin` subdirectory
of your build folder.

You will end up in YCBS's CLI, with the YcsbDBClient loaded as the
DB backend, itself using a test implementation of an in-memory database
with which you can interact (type `help` to see a list of available commands).

## Writing your own C++ DB backend

ycsb-cpp-interface provides a header file, `YCSBCppInterface.hpp`, with
a `ycsb::DB` abstract class. To implement your own C++ backend database,
you simply need to implement a child class of the `ycsb::DB` class that
implements the required virtual functions. You may look at [](src/TestDB.cpp)
as an example of such an implementation. Note the use of the
`YCSB_CPP_REGISTER_DB_TYPE` macro after the class definition. This macro
must be called in a .cpp file to associate the name of your backend
(e.g. `myawesomedb`) with the class name to use (e.g., `MyAwesomeDB`).

Once your database class is ready, compile it into a shared library
(e.g., `libmyawesomedb.so`). Make sure the `LD_LIBRARY_PATH` environment
variable contains the path to your dynamic library. You may then test
your backend with the CLI as follows.

```
$ ycsb-cpp-cli -p ycsb.cpp.library=libmyawesomedb.so -p ycsb.cpp.backend=myawesomedb
```

The `ycsb.cpp.library` and `ycsb.cpp.backend` properties are the only properties
needed by ycsb-cpp-interface. Any other properties provided will be propagated
to your database implementation in the form of an `std::unordered_map<std::string, std::string>`.
Note that `ycsb.cpp.library` may accept a full path to your dynamic library,
if you don't want to change the `LD_LIBRARY_PATH` environment variable.

## Running YCSB with your C++ DB backend

ycsb-cpp-interface provides a convenience script, `ycsb-cpp`, to run YCSB
with your own backend. It can be used in a way similar to the original ycsb script,
as follows.

```
$ ycsb-cpp load -p ycsb.cpp.library=libmyawesomedb.so -p ycsb.cpp.backend=myawesomedb -P workloadfile
$ ycsb-cpp run -p ycsb.cpp.library=libmyawesomedb.so -p ycsb.cpp.backend=myawesomedb -P workloadfile
```
