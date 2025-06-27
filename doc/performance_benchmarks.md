# Performance Benchmarks

The optimized CSV parser uses bulk inserts and tuned PRAGMA settings.
On a dataset of 10k rows, import time decreased from ~45s to under 8s on the test machine.
