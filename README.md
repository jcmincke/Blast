# Blast: Distributed Computing in Haskell

**Blast** is a pure Haskell library for doing distributed computing. It has the following characteristics:

* Works on any RPC backend. The current implementation runs on both local threads and CloudHaskell.
* Is based on 5 simple primitives, allowing the user to define his own syntax above them.
* Has a buit-in fail/safe mechanism in case of slave failure.
* Transparently works with both stateless and stateful slaves.
* Automatically handles slave caching.


## Getting started

```
$ stack build
```

Builds the library and the examples.

## Examples

Each example can be run on 2 backends. 
* A local backend that uses local threads.
* A CloudHaskell backend.

To run an example on CloudHaskell:
* Edit Main.hs and choose the right CloudHaskell main function (the one that is suffixed with CH)
* Starts as many slaves as needed.

```
$ stack exec -- example-name slave host port
```
* Starts the master
```
$ stack exec -- example-name master host port
```

The following commands starts the KMean example with 2 slaves.

```
$ stack exec -- kmeans slave localhost 5001
$ stack exec -- kmeans slave localhost 5002
$ stack exec -- kmeans master localhost 5000
```

### Simple

A set a simple examples illustrating remote mapping and folding as well as an iterative distributed algorithm.

```
$ stack exec -- simple
```

### Kmeans

Implementation of the distributed KMean algorithm.

```
$ stack exec -- kmeans
```

### WordCount

Counts the occurence of each character in multiple files.

```
$ cd examples/WordCount
$ stack exec -- wordcount
```


## License

Copyright (c) 2016-2017 Jean-Christophe Mincke.

All rights reserved.

Blast is free software, and may be redistributed under the terms
specified in the [LICENSE](LICENSE) file.