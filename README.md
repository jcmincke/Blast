# Blast: Distributed Computing in Haskell

*Blast:* a pure Haskell library for doing distributed computing. It has the following characteristics:

* Works on any RPC backend. The current implementation runs on local thread and on CloudHaskell.
* Based on 5 simple primitives, allowing the user to define his own syntax above them.
* Has a buit-in fail/safe mechanism in case of slave failure.
* Transparently works with both stateless and stageful slaves.
* Automatically handles slave caching.


## Getting started

```
$ stack build
```

Builds the library and the examples.

## Examples

Each example can be run on 2 backend. 
* A local backend that uses local threads

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
$ stack exec -- wordcount
```


## License

Copyright (c) 2016-2017 Jean-Christophe Mincke.

All rights reserved.

Blast is free software, and may be redistributed under the terms
specified in the [LICENSE](LICENSE) file.