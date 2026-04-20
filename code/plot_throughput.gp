set terminal pdf size 11in,7in
set output "throughput.pdf"

set title "IPC Throughput vs Message Size"
set xlabel "Message size"
set ylabel "Throughput (MiB/s)"

set grid
set logscale x 2

# Больше места снизу и сверху
set lmargin 10
set rmargin 4
set tmargin 4
set bmargin 5

# Легенда сверху, вне области графика
set key outside top center horizontal

# Чуть реже подписи по X, чтобы справа не слипались
set xtics ("1 B" 1, \
           "32 B" 32, \
           "1 KiB" 1024, \
           "32 KiB" 32768, \
           "1 MiB" 1048576, \
           "128 MiB" 134217728)

plot \
    "results_10.dat" using 1:7 with linespoints title "repeat = 10", \
    "results_100.dat" using 1:7 with linespoints title "repeat = 100", \
    "results_1000.dat" using 1:7 with linespoints title "repeat = 1000"