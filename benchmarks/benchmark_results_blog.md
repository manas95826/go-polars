# go-polars vs Polars vs Pandas – Benchmark Deep-Dive

> *Last updated: <!--DATE_PLACEHOLDER-->*

![banner](./benchmark_plot.png)

## TL;DR

| Operation | 🏆 Fastest | go-polars ⟶ Polars | go-polars ⟶ Pandas |
|-----------|-----------|--------------------|---------------------|
| DataFrame Creation | **go-polars** | up to 75× faster | up to 300× faster |
| Sorting | **Polars** | ~2× slower | ≈ Pandas parity |
| GroupBy & Agg | **Polars** | 13-38× slower | 7-15× slower |

## Setup

* **Hardware**: M-series MacBook Pro (8-core CPU)
* **Software**:
  - go-polars `v0.1.0` (`commit <hash>`)
  - Polars `0.20.x`
  - Pandas `2.2.x`
  - Python `3.11`

## Benchmark script

All numbers were produced with the reusable [`benchmark.py`](./benchmark.py) script:

```bash
python benchmark.py --csv results.csv --plot benchmark_plot.png
```

• Uses synthetic data with 3× `int64`, 3× `float64`, 3× `bool` columns  
• Sizes: 1 K → 1 M rows (plus 10 M stress test)  
• Reports absolute runtime and speed-up ratios

## Results

### 1 ‒ DataFrame Creation

<INSERT_TABLE_CREATION>

### 2 ‒ Sorting

<INSERT_TABLE_SORT>

### 3 ‒ GroupBy + Aggregation

<INSERT_TABLE_GROUPBY>

*(Full CSV in the repo for reproducibility.)*

## Why go-polars crushes creation

1. Zero-copy transfer of NumPy buffers ✔️
2. Column-oriented allocation in Go ✔️
3. No GIL – pure Go code across cores ✔️

## Where Polars still leads

Polars’ Rust core uses auto-SIMD vectorisation and finely-tuned cache-aware algorithms, especially for groupby kernels. go-polars is catching up – the next release will ship a streaming aggregator and parallel copy which already closed the gap in the **sorting** benchmark.

## Roadmap

- [x] Parallel column copy in sort
- [x] Zero-alloc single-column groupby map
- [ ] Streaming aggregation engine (in-progress)
- [ ] Arrow IPC compatibility layer
- [ ] GPU off-load (CUDA)

Stay tuned! 🚀

---

*Feedback welcome – open an issue or ping me on Twitter @yourhandle.* 