set terminal pdf
set output "throughput.pdf"

set title "IPC Throughput vs Message Size"
set xlabel "Message size (bytes)"
set ylabel "Throughput (MiB/s)"
set grid
set logscale x 2

plot "results.dat" using 1:8 with linespoints title "Throughput"