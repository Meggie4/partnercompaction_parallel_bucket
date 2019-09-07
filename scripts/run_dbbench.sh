#!/bin/bash
#set -x

NUMTHREAD=1
#BENCHMARKS="
#customed99hot256_100k,\
#customed80hot256_100k,\
#customed99hot1k_100k,\
#customed80hot1k_100k,\
#customeduniform1k_100k,\
#customed99hot4k_100k,\
#customed80hot4k_100k,\
#customeduniform4k_100k,\
#customed99hot1k_500k,\
#customed80hot1k_500k,\
#customeduniform1k_500k,\
#customed99hot4k_500k,\
#customed80hot4k_500k,\
#customeduniform4k_500k,\
#customed99hot256_1000k,\
#customed80hot256_1000k,\
#customed99hot1k_1000k,\
#customed80hot1k_1000k,\
#customeduniform1k_1000k,\
#customed99hot4k_1000k,\
#customed80hot4k_1000k,\
#customeduniform4k_1000k,\
#customedlatest1k_100k,\
#customedlatest1k_1000k"

#BENCHMARKS="customed99hot256_100k"
#BENCHMARKS="customed80hot256_100k"
#BENCHMARKS="customed99hot1k_100k"
#BENCHMARKS="customed80hot1k_100k"
#BENCHMARKS="customeduniform1k_100k"
#BENCHMARKS="customed99hot4k_100k"
#BENCHMARKS="customed80hot4k_100k"
#BENCHMARKS="customeduniform4k_100k"
#BENCHMARKS="customed99hot1k_500k"
#BENCHMARKS="customed80hot1k_500k"
#BENCHMARKS="customeduniform1k_500k"
#BENCHMARKS="customed99hot4k_500k"
#BENCHMARKS="customed80hot4k_500k"
#BENCHMARKS="customeduniform4k_500k"
#BENCHMARKS="customed99hot256_1000k"
#BENCHMARKS="customed80hot256_1000k"
#BENCHMARKS="customed99hot1k_1000k"
#BENCHMARKS="customed80hot1k_1000k"
#BENCHMARKS="customeduniform1k_1000k"
#BENCHMARKS="customed99hot4k_1000k"
#BENCHMARKS="customed80hot4k_1000k"
#BENCHMARKS="customeduniform4k_1000k"
#BENCHMARKS="customedlatest1k_100k"
#BENCHMARKS="customedlatest1k_1000k"

#BENCHMARKS="loaduniform100_1000k,readuniform100_1000k"
#BENCHMARKS="loaduniform100_5000k,readuniform100_5000k"
#BENCHMARKS="loaduniform100_10000k,readuniform100_10000k"
#BENCHMARKS="loaduniform100_15000k,readuniform100_15000k"
#BENCHMARKS="loaduniform100_20000k,readuniform100_20000k"
#BENCHMARKS="loaduniform100_25000k,readuniform100_25000k"
#BENCHMARKS="loaduniform100_30000k,readuniform100_30000k"

#BENCHMARKS="loadzipfian100_100k,readzipfian100_100k"
#BENCHMARKS="loadzipfian100_200k,readzipfian100_200k"
#BENCHMARKS="loadzipfian100_500k,readzipfian100_500k"
#BENCHMARKS="loadzipfian100_1000k,readzipfian100_1000k"
BENCHMARKS="loadzipfian100_5000k,readzipfian100_5000k"
#BENCHMARKS="loadzipfian100_10000k,readzipfian100_10000k"
#BENCHMARKS="loadzipfian100_15000k,readzipfian100_15000k"
#BENCHMARKS="loadzipfian100_20000k,readzipfian100_20000k"
#BENCHMARKS="loadzipfian100_25000k,readzipfian100_25000k"
#BENCHMARKS="loadzipfian100_30000k,readzipfian100_30000k"

#BENCHMARKS="loadzipfian100_5000k"

#BENCHMARKS="loaduniform100_5000k"
#BENCHMARKS="loaduniform100_10000k"
#BENCHMARKS="loaduniform100_15000k"
#BENCHMARKS="loaduniform100_20000k"
#BENCHMARKS="loaduniform100_25000k"
#BENCHMARKS="loaduniform100_30000k"

#BENCHMARKS="writezipfian100_5000k"
#BENCHMARKS="writezipfian100_10000k"
#BENCHMARKS="writezipfian100_20000k"
#BENCHMARKS="writezipfian100_25000k"

#BENCHMARKS="fillrandom,readrandom"
#BENCHMARKS="fillrandom"

#NoveLSM specific parameters
#NoveLSM uses memtable levels, always set to num_levels 2
#write_buffer_size DRAM memtable size in MBs
#write_buffer_size_2 specifies NVM memtable size; set it in few GBs for perfomance;
OTHERPARAMS="--write_buffer_size=$DRAMBUFFSZ"

#valgrind --verbose --log-file=valgrind --leak-check=full  --show-leak-kinds=all $DBBENCH/db_bench --threads=$NUMTHREAD --benchmarks=$BENCHMARKS $OTHERPARAMS
$DBBENCH/db_bench --threads=$NUMTHREAD --benchmarks=$BENCHMARKS $OTHERPARAMS

#Run all benchmarks
#$APP_PREFIX $DBBENCH/db_bench --threads=$NUMTHREAD --num=$NUMKEYS --value_size=$VALUSESZ \
#$OTHERPARAMS --num_read_threads=$NUMREADTHREADS

