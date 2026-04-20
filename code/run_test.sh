#!/bin/bash

RAW_OUT="results_raw.txt"
OUT10="results_10.dat"
OUT100="results_100.dat"
OUT1000="results_1000.dat"

> "$RAW_OUT"
> "$OUT10"
> "$OUT100"
> "$OUT1000"

SIZES=(
1 2 4 8 16 32 64 128 256 512
1024 2048 4096 8192 16384 32768 65536 131072 262144 524288
1048576 2097152 4194304 8388608 16777216 33554432 67108864 134217728
)

for repeat in 10 100 1000
do
    echo "===== repeat=$repeat =====" | tee -a "$RAW_OUT"

    for size in "${SIZES[@]}"
    do
        echo "Running size=$size repeat=$repeat"
        ./main 2 "$size" "$repeat" | tee -a "$RAW_OUT"
    done
done

grep "^\[data\].* 10 " "$RAW_OUT" | sed 's/\[data\] //' > "$OUT10"
grep "^\[data\].* 100 " "$RAW_OUT" | sed 's/\[data\] //' > "$OUT100"
grep "^\[data\].* 1000 " "$RAW_OUT" | sed 's/\[data\] //' > "$OUT1000"