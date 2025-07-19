import os, psutil, sys, subprocess

MAX_MEM_GB = 8  # Abort if system memory usage exceeds this during test

if __name__ == "__main__":
    process = psutil.Process(os.getpid())

    print("Starting 10M-row stress benchmark…")

    cmd = [sys.executable, 'benchmark.py', '--stress', '--csv', 'stress_results.csv']
    proc = subprocess.Popen(cmd)

    try:
        while proc.poll() is None:
            mem = process.memory_info().rss / (1024 ** 3)
            if mem > MAX_MEM_GB:
                proc.kill()
                print(f"Aborted: memory usage exceeded {MAX_MEM_GB} GB")
                sys.exit(1)
    except KeyboardInterrupt:
        proc.kill()
        sys.exit(1)

    print("Stress benchmark completed. CSV written to stress_results.csv") 