# Benchmarks

This directory contains benchmark-related files and results for go-polars.

## Running Benchmarks

To run the benchmarks:

```bash
# Run standard benchmarks
python benchmark.py --csv results.csv --plot results.png

# Run stress tests
python stress_test.py
```

## Files

- `benchmark.py`: Main benchmark script comparing go-polars with Polars and Pandas
- `stress_test.py`: Stress testing script for large datasets
- `results.csv`: Latest benchmark results
- `results.png`: Visualization of benchmark results
- `stress_results.csv`: Results from stress testing

## Methodology

The benchmarks test three main operations:
1. DataFrame Creation
2. Sorting
3. GroupBy Operations

Each operation is tested with different dataset sizes:
- 1K rows
- 10K rows
- 100K rows
- 1M rows

The data consists of:
- 3 integer columns
- 3 float columns
- 3 boolean columns 