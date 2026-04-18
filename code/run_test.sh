#!/bin/bash

OUT="results_raw.txt"
> "$OUT"

for size in \
1 2 4 8 16 32 64 128 256 512 \
1024 2048 4096 8192 16384 32768 65536 \
131072 262144 524288 \
1048576 2097152 4194304 8388608 16777216 33554432 67108864 
do
    echo "Running size=$size"
    ./main 2 "$size" >> "$OUT"
done