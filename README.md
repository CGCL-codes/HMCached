# HMCached: An In-Memory Key-Value Store on Hybrid DRAM/NVM Memories

&#160; &#160; &#160; &#160; HMCached is a in-memory key-value store built on a DRAM/NVM hybrid memory system. HMCached develops an application-level data access accounting mechanism to track hotness of
objects on NVM, and migrates them to fast DRAM based on dynamic hotness threshold adjustment. HMCached adopts slab-based memory allocation and solves the slab calcification problem
with an effective DRAM repartition strategy, and thus significantly enhance the performance gain from the small-size DRAM. Moreover, we propose a NVM-friendly index structure to
further mitigate data accesses to NVM. Compared to previous studies, our hot data migration policy is implemented at the application level, without modifying hardware and operating
systems. We implement the proposed schemes with Memcached.

HMCached Setup, Compiling, Configuration and How to use
------------

See: https://memcached.org/about

A fun story explaining usage: https://memcached.org/tutorial

If you're having trouble, try the wiki: https://memcached.org/wiki

If you're trying to troubleshoot odd behavior or timeouts, see:
https://memcached.org/timeouts

https://memcached.org/ is a good resource in general. Please use the mailing
list to ask questions, github issues aren't seen by everyone!

## Dependencies

* libevent, http://www.monkey.org/~provos/libevent/ (libevent-dev)
* libseccomp, (optional, linux) - enables process restrictions for better
  security.

## Environment

Be warned that the -k (mlockall) option to memcached might be
dangerous when using a large cache.  Just make sure the memcached machines
don't swap.  memcached does non-blocking network I/O, but not disk.  (it
should never go to disk, or you've lost the whole point of it)

## Website

* http://www.memcached.org

## Contributing

See https://github.com/memcached/memcached/wiki/DevelopmentRepos
