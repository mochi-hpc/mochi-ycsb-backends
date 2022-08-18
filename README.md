# Mochi YCSB Backends

This repository contains backends implemented with
[mochi](https://www.mcs.anl.gov/research/projects/mochi/)
for the [YCSB benchmark](https://github.com/brianfrankcooper/YCSB).

## Installing

### Installing using Spack

As with all Mochi-based libraries, the best way to build and install
this project is to use the [Spack](https://spack.io/) package manager,
following [these instructions](https://mochi.readthedocs.io/en/latest/installing.html)
to enable the Mochi namespace, and by executing the following command.

```
$ spack install mochi-ycsb-benchmarks
```

### Building manually

To build this repository from source, you will first need to have
spack installed and the Mochi namespace setup to be able to install
the required dependencies. You can then clone this repository and
install a local spack environment using the provided `spack.yaml` file
as follows.

```
$ cd mochi-ycsb-benchmarks
$ spack env create -d .
$ spack env activate .
$ spack install
```

You will then have to set the `JAVA_HOME` environment variable
to point to the `openjdk` package that Spack installed, as follows.

```
export JAVA_HOME=`spack location -i openjdk`
```

You can then build the source contained in this repository as follows.

```
$ mkdir build
$ cd build
$ cmake .. -DYCSB_ROOT=`spack location -i ycsb`
$ make
```
