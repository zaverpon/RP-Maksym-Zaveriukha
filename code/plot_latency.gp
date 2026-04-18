set terminal pdf
set output "latency.pdf"

set title "IPC Roundtrip Latency vs Message Size"
set xlabel "Message size (bytes)"
set ylabel "Latency (us)"
set grid
set logscale x 2

plot "results.dat" using 1:7 with linespoints title "Latency"