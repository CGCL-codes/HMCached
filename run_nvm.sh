#!/bin/bash

_PREFIX="/home/HME-test"

_INPUT_COMMAN="./memcached -l 11.11.11.5 -p 11211 -t 4"

_LOAD_PATH="/home/HME-test/scripts/setupdev.sh"

_RUN_PATH="/home/HME-test/scripts/runenv.sh"

_ROOT="sudo"

_LOAD="load"

_RELOAD="reload"

echo ${_INPUT_COMMAN}
#Init()
#{
#    ${_ROOT} ${_LOAD_PATH} ${_RELOAD}
#    ${_ROOT} ${_LOAD_PATH} ${_LOAD}
#}

Start()
{
    numactl --cpunodebind=0 --membind=0 ${_RUN_PATH} ${_INPUT_COMMAN}
}

#Init
Start
