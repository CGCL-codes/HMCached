#!/bin/bash

#numactl --physcpubind=0 --membind=0 
#numactl --cpunodebind=0 --membind=0 ./memcached -l 11.11.11.5 -p 11211 -t 4
numactl --physcpubind=0,1,2,3,4,5 --membind=0 ./memcached -l 11.11.11.5 -p 11211 -t 4
