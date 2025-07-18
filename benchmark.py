import numpy as np
import gopolars as gp
import pandas as pd
import time
import gc

def benchmark_load(size, num_cols=3):
    # Generate test data
    data = {}
    for i in range(num_cols):
        if i % 3 == 0:
            data[f'int_col_{i}'] = np.random.randint(0, 1000, size=size, dtype=np.int64)
        elif i % 3 == 1:
            data[f'float_col_{i}'] = np.random.random(size).astype(np.float64)
        else:
            data[f'bool_col_{i}'] = np.random.choice([True, False], size=size)
    
    # Benchmark gopolars
    gc.collect()  # Clear memory before test
    start_time = time.time()
    df_gp = gp.DataFrame.from_dict(data)
    gp_time = time.time() - start_time
    
    # Benchmark pandas
    gc.collect()  # Clear memory before test
    start_time = time.time()
    df_pd = pd.DataFrame(data)
    pd_time = time.time() - start_time
    
    return gp_time, pd_time

# Test different sizes
sizes = [1000, 10000, 100000, 1000000]
num_cols = 9  # 3 of each type (int64, float64, bool)

print(f"{'Size':>10} | {'Columns':>7} | {'GoPolars (s)':>12} | {'Pandas (s)':>10} | {'Ratio':>10}")
print("-" * 60)

for size in sizes:
    gp_time, pd_time = benchmark_load(size, num_cols)
    ratio = gp_time / pd_time
    print(f"{size:>10} | {num_cols:>7} | {gp_time:>12.4f} | {pd_time:>10.4f} | {ratio:>10.2f}") 