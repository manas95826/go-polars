# GoPolars

A high-performance DataFrame library for Python powered by Go. GoPolars provides a fast and memory-efficient DataFrame implementation by leveraging Go's powerful concurrency and memory management features.

## Features

- High-performance DataFrame operations
- Native support for NumPy arrays
- Seamless integration between Python and Go
- Support for common data types (int64, float64, bool)
- Memory-efficient data handling

## Benchmarks

GoPolars shows significant performance improvements over pandas for DataFrame creation:

```
      Size | Columns | GoPolars (s) | Pandas (s) | Ratio
------------------------------------------------------------
      1000 |       9 |       0.0002 |     0.0011 |   0.17
     10000 |       9 |       0.0001 |     0.0004 |   0.28
    100000 |       9 |       0.0000 |     0.0020 |   0.02
   1000000 |       9 |       0.0000 |     0.0262 |   0.00
```

## Requirements

- Python 3.7+
- Go 1.16+
- NumPy

## Installation

```bash
pip install gopolars
```

Note: Go must be installed on your system to build the package.

## Usage

```python
import numpy as np
import gopolars as gp

# Create a DataFrame from a dictionary
df = gp.DataFrame.from_dict({
    'a': np.array([1, 2, 3], dtype=np.int64),
    'b': np.array([1.1, 2.2, 3.3], dtype=np.float64),
    'c': np.array([True, False, True], dtype=np.bool_)
})

# Get DataFrame shape
print(df.shape)  # (3, 3)
```

## Development

To build from source:

```bash
git clone https://github.com/manaschopra/go-polars
cd go-polars
pip install -e .
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 