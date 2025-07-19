import argparse
import gc
import time
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl

import go_polars as gp
from go_polars import AggType

# ----------------------------------------------------------------------------
# CLI helpers
# ----------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark go-polars vs Polars vs Pandas")
    p.add_argument("--csv", type=str, default=None, help="Path to save CSV results")
    p.add_argument("--plot", type=str, default=None, help="Path to save PNG plot")
    p.add_argument("--max_rows", type=int, default=1_000_000, help="Maximum number of rows to test")
    p.add_argument("--stress", action="store_true", help="Run a stress test up to 10M rows (may use a lot of RAM!)")
    return p.parse_args()

# ----------------------------------------------------------------------------
# Benchmark helpers (unchanged logic)
# ----------------------------------------------------------------------------

def generate_test_data(size, num_cols=3):
    """Generate test data for benchmarking"""
    data = {}
    for i in range(num_cols):
        if i % 3 == 0:
            data[f'int_col_{i}'] = np.random.randint(0, 10, size=size, dtype=np.int64)  # Smaller range for better grouping
        elif i % 3 == 1:
            data[f'float_col_{i}'] = np.random.random(size).astype(np.float64)
        else:
            data[f'bool_col_{i}'] = np.random.choice([True, False], size=size)
    return data

def benchmark_load(size, num_cols=3):
    """Benchmark DataFrame creation"""
    data = generate_test_data(size, num_cols)
    
    # Benchmark go-polars
    gc.collect()  # Clear memory before test
    start_time = time.time()
    df_gp = gp.DataFrame.from_dict(data)
    gp_time = time.time() - start_time
    
    # Benchmark polars
    gc.collect()  # Clear memory before test
    start_time = time.time()
    df_pl = pl.DataFrame(data)
    pl_time = time.time() - start_time
    
    # Benchmark pandas
    gc.collect()  # Clear memory before test
    start_time = time.time()
    df_pd = pd.DataFrame(data)
    pd_time = time.time() - start_time
    
    return gp_time, pl_time, pd_time

def benchmark_sort(size, num_cols=3):
    """Benchmark sorting operations"""
    data = generate_test_data(size, num_cols)
    df_gp = gp.DataFrame.from_dict(data)
    df_pl = pl.DataFrame(data)
    df_pd = pd.DataFrame(data)
    sort_col = 'int_col_0'

    # Benchmark go-polars
    gc.collect()
    start_time = time.time()
    df_gp.sort_values(by=sort_col)
    gp_time = time.time() - start_time

    # Benchmark polars
    gc.collect()
    start_time = time.time()
    df_pl.sort(sort_col)
    pl_time = time.time() - start_time

    # Benchmark pandas
    gc.collect()
    start_time = time.time()
    df_pd.sort_values(by=sort_col)
    pd_time = time.time() - start_time

    return gp_time, pl_time, pd_time

def benchmark_groupby(size, num_cols=3):
    """Benchmark groupby and aggregation operations"""
    data = generate_test_data(size, num_cols)
    df_gp = gp.DataFrame.from_dict(data)
    df_pl = pl.DataFrame(data)
    df_pd = pd.DataFrame(data)
    group_col = 'int_col_0'
    agg_col = 'float_col_1'

    # Benchmark go-polars
    gc.collect()
    start_time = time.time()
    df_gp.groupby(group_col).agg(agg_col, AggType.MEAN)
    gp_time = time.time() - start_time

    # Benchmark polars
    gc.collect()
    start_time = time.time()
    if hasattr(df_pl, 'groupby'):
        df_pl.groupby(group_col).agg(pl.col(agg_col).mean())
    else:
        df_pl.group_by(group_col).agg(pl.col(agg_col).mean())
    pl_time = time.time() - start_time

    # Benchmark pandas
    gc.collect()
    start_time = time.time()
    df_pd.groupby(group_col)[agg_col].mean()
    pd_time = time.time() - start_time

    return gp_time, pl_time, pd_time

def run_benchmarks():
    """Run all benchmarks and return structured result list."""
    args = parse_args()

    max_rows = args.max_rows
    if args.stress:
        # exponential up to 10M
        sizes = [10_000, 100_000, 1_000_000, 10_000_000]
    else:
        sizes = [1_000, 10_000, 100_000, max_rows]

    num_cols = 9  # 3 of each type (int64, float64, bool)

    records: List[dict] = []

    print("\n=== DataFrame Creation Benchmark ===")
    header = f"{'Size':>10} | {'Columns':>7} | {'go-polars (s)':>12} | {'Polars (s)':>10} | {'Pandas (s)':>10} | {'GP/PL':>8} | {'GP/PD':>8}"
    print(header)
    print("-" * len(header))
    for size in sizes:
        gp_time, pl_time, pd_time = benchmark_load(size, num_cols)
        ratio_pl = gp_time / pl_time
        ratio_pd = gp_time / pd_time
        print(f"{size:>10} | {num_cols:>7} | {gp_time:>12.4f} | {pl_time:>10.4f} | {pd_time:>10.4f} | {ratio_pl:>8.2f} | {ratio_pd:>8.2f}")

        records.append({
            'rows': size,
            'operation': 'create',
            'gp': gp_time,
            'pl': pl_time,
            'pd': pd_time,
        })

    print("\n=== Sorting Benchmark ===")
    print(header)
    print("-" * len(header))
    for size in sizes:
        gp_time, pl_time, pd_time = benchmark_sort(size, num_cols)
        ratio_pl = gp_time / pl_time
        ratio_pd = gp_time / pd_time
        print(f"{size:>10} | {num_cols:>7} | {gp_time:>12.4f} | {pl_time:>10.4f} | {pd_time:>10.4f} | {ratio_pl:>8.2f} | {ratio_pd:>8.2f}")

        records.append({
            'rows': size,
            'operation': 'sort',
            'gp': gp_time,
            'pl': pl_time,
            'pd': pd_time,
        })

    print("\n=== GroupBy and Aggregation Benchmark ===")
    print(header)
    print("-" * len(header))
    for size in sizes:
        gp_time, pl_time, pd_time = benchmark_groupby(size, num_cols)
        ratio_pl = gp_time / pl_time
        ratio_pd = gp_time / pd_time
        print(f"{size:>10} | {num_cols:>7} | {gp_time:>12.4f} | {pl_time:>10.4f} | {pd_time:>10.4f} | {ratio_pl:>8.2f} | {ratio_pd:>8.2f}")

        records.append({
            'rows': size,
            'operation': 'groupby',
            'gp': gp_time,
            'pl': pl_time,
            'pd': pd_time,
        })

    # ------------------------------------------------------------------
    # Optional CSV export
    # ------------------------------------------------------------------
    if args.csv:
        import csv

        csv_path = Path(args.csv)
        with csv_path.open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['operation', 'rows', 'gp', 'pl', 'pd'])
            writer.writeheader()
            writer.writerows(records)
        print(f"\nCSV results written to {csv_path.resolve()}")

    # ------------------------------------------------------------------
    # Optional Plot
    # ------------------------------------------------------------------
    if args.plot:
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        op_to_ax = {'create': 0, 'sort': 1, 'groupby': 2}
        for op in ('create', 'sort', 'groupby'):
            ax = axes[op_to_ax[op]]
            xs = [r['rows'] for r in records if r['operation'] == op]
            ax.plot(xs, [r['gp'] for r in records if r['operation'] == op], label='go-polars')
            ax.plot(xs, [r['pl'] for r in records if r['operation'] == op], label='polars')
            ax.plot(xs, [r['pd'] for r in records if r['operation'] == op], label='pandas')
            ax.set_title(op.capitalize())
            ax.set_xlabel('Rows')
            ax.set_ylabel('Seconds')
            ax.set_xscale('log')
            ax.set_yscale('log')
            ax.legend()

        plt.tight_layout()

        plot_path = Path(args.plot)
        fig.savefig(plot_path, dpi=150)
        print(f"Plot saved to {plot_path.resolve()}")

    return records

if __name__ == "__main__":
    run_benchmarks() 