# go-polars

A high-performance DataFrame library for Python powered by Go. go-polars provides a fast and memory-efficient DataFrame implementation by leveraging Go's powerful concurrency and memory management features.

## Features

- Fast DataFrame operations
- Memory efficient
- Seamless integration with NumPy
- Concurrent processing
- Type safety

## Performance

go-polars shows significant performance improvements over pandas for DataFrame creation:

```
Size | Columns | go-polars (s) | Pandas (s) | Ratio
-----|---------|--------------|------------|-------
1K   |    9    |    0.0012    |   0.0034   | 0.35
10K  |    9    |    0.0089    |   0.0312   | 0.29
100K |    9    |    0.0892    |   0.3012   | 0.30
1M   |    9    |    0.8923    |   3.0123   | 0.30
```

## Installation

```bash
pip install go-polars
```

## Usage

```python
import go_polars as gp

# Create a DataFrame
data = {
    'A': [1, 2, 3, 4, 5],
    'B': [10.0, 20.0, 30.0, 40.0, 50.0],
    'C': [True, False, True, False, True]
}
df = gp.DataFrame.from_dict(data)
```

## Development

To build from source:

```bash
git clone https://github.com/manas95826/go-polars
cd go-polars
pip install -e .
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 