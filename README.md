# HMCached: An In-Memory Key-Value Store on Hybrid DRAM/NVM Memories

&#160; &#160; &#160; &#160; HMCached is a in-memory key-value store built on a DRAM/NVM hybrid memory system. HMCached develops an application-level data access accounting mechanism to track hotness of
objects on NVM, and migrates them to fast DRAM based on dynamic hotness threshold adjustment. HMCached adopts slab-based memory allocation and solves the slab calcification problem
with an effective DRAM repartition strategy, and thus significantly enhance the performance gain from the small-size DRAM. Moreover, we propose a NVM-friendly index structure to
further mitigate data accesses to NVM. Compared to previous studies, our hot data migration policy is implemented at the application level, without modifying hardware and operating
systems. We implement the proposed schemes with Memcached.

HMCached Setup, Compiling, Configuration and How to use
------------

## 1.Dependencies

* libevent, http://www.monkey.org/~provos/libevent/ (libevent-dev)
* libseccomp, (optional, linux) - enables process restrictions for better
  security.
* numactl-devel

## 2.Compiling

```javascript
[root @node1 HMCached]# cd dram_repartition
[root @node1 dram_repartition]# make
[root @node1 HMCached]# cp libdram_repartition.so /usr/lib
[root @node1 HMCached]# cd HMCached
[root @node1 HMCached]# autoreconf -fis 
[root @node1 HMCached]# ./configure
[root @node1 HMCached]# make
```

# 3.Configuration


# 4.Running


# 5. Available Benchmarks

* Yahoo! Cloud Serving Benchmark, https://github.com/brianfrankcooper/YCSB
* Mutilate: high-performance memcached load generator, https://github.com/leverich/mutilate

Support or Contact
------------
If you have any questions, please contact Zhiwei Li(leezw@hust.edu.cn), Haikun Liu (hkliu@hust.edu.cn) and Xiaofei Liao (xfliao@hust.edu.cn).
