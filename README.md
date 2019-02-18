# HMCached: An In-Memory Key-Value Store on Hybrid DRAM/NVM Memories

&#160; &#160; &#160; &#160; HMCached is a in-memory key-value store built on a DRAM/NVM hybrid memory system. HMCached develops an application-level data access accounting mechanism to track hotness of
objects on NVM, and migrates them to fast DRAM based on dynamic hotness threshold adjustment. HMCached adopts slab-based memory allocation and solves the slab calcification problem
with an effective DRAM repartition strategy, and thus significantly enhance the performance gain from the small-size DRAM. Moreover, we propose a NVM-friendly index structure to
further mitigate data accesses to NVM. Compared to previous studies, our hot data migration policy is implemented at the application-level, without modifying hardware and operating
systems. 

We implement the proposed system with Memcached (https://memcached.org/). 

<!--
Compared on the vanilla Memcached, HMCached has added the following functions:
* **Architecture**:
* **NVM-friendly Index Structure**:
* **Hotness-Aware Object Migration**:
* **Slab-based memory management**:
-->

HMCached Usage
------------

### 1.External Dependencies

* NUMA system: HMCached must runs in a non-uniform memory access (NUMA) system with multiple NUMA nodes,
               and each node presents a type of memory mediums.
* NVM device: If without available NVM device, you can use HME for emulation, https://github.com/CGCL-codes/HME.
* numactl-devel
* libevent: http://www.monkey.org/~provos/libevent/ (libevent-dev)
* libseccomp (optional, linux): enables process restrictions for better
  security.

### 2.Compiling

```javascript
[user @node1 HMCached]$ cd dram_repartition
[user @node1 dram_repartition]$ make
[user @node1 HMCached]$ sudo cp libdram_repartition.so /usr/lib
[user @node1 HMCached]$ cd HMCached
[user @node1 HMCached]$ autoreconf -fis 
[user @node1 HMCached]$ ./configure
[user @node1 HMCached]$ make
```

### 4.Running

The run mode of HMCached is similar to Memcached.

#### Server-side:

The following operation is based on an assumption that memory in Node 0 is DRAM while memory in Node 1 is NVM.
```javascript
[user @node1 HMCached]$ numactl --cpunodebind=0 --membind=0 ./memcached -l 127.1.1.1 -p 11211
```

#### Client-side (e.g., running with telnet):
```javascript
[user @node1 home]$ telnet 127.1.1.1 11211
Trying 127.1.1.1...
Connected to localhost.
Escape character is '^]'.
get foo
VALUE foo 0 2
hi
END
stats
STAT pid 8861
(etc)
```

### 5. Benchmarks

* Yahoo! Cloud Serving Benchmark, https://github.com/brianfrankcooper/YCSB
* Mutilate: high-performance memcached load generator, https://github.com/leverich/mutilate

Support or Contact
------------
HMCached is developed at SCTS&CGCL Lab (http://grid.hust.edu.cn/) by Zhiwei Li, Haikun Liu and Xiaofei Liao. 
For any questions, please contact Zhiwei Li(leezw@hust.edu.cn),
Haikun Liu (hkliu@hust.edu.cn) and Xiaofei Liao (xfliao@hust.edu.cn).
