# Go-Polars: Fast DataFrame Library for Go

Go-Polars is a high-performance DataFrame library for Go, inspired by the Rust-based Polars library. It provides efficient data manipulation capabilities with a focus on performance and ease of use.

## Features

- Type-safe Series and DataFrame operations
- Support for common data types (int64, float64, string, bool)
- Basic DataFrame operations (select, filter, head)
- Memory-efficient data handling
- Easy-to-use API

## Installation

```bash
go get github.com/manaschopra/go-polars
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/manaschopra/go-polars/dataframe"
    "github.com/manaschopra/go-polars/types"
)

func main() {
    // Create series
    ages := types.NewSeries("age", []int64{25, 30, 35, 40})
    names := types.NewSeries("name", []string{"Alice", "Bob", "Charlie", "David"})
    scores := types.NewSeries("score", []float64{95.5, 85.0, 92.5, 88.0})

    // Create DataFrame
    series := map[string]*types.Series{
        "age": ages,
        "name": names,
        "score": scores,
    }
    df, err := dataframe.New(series)
    if err != nil {
        panic(err)
    }

    // Print DataFrame dimensions
    rows, cols := df.Shape()
    fmt.Printf("DataFrame shape: (%d, %d)\n", rows, cols)

    // Select specific columns
    selected, err := df.Select([]string{"name", "age"})
    if err != nil {
        panic(err)
    }

    // Filter rows
    filtered, err := df.Filter("age", func(v interface{}) bool {
        age := v.(int64)
        return age > 30
    })
    if err != nil {
        panic(err)
    }

    // Get first 2 rows
    head, err := df.Head(2)
    if err != nil {
        panic(err)
    }
}
```

## Features in Development

- Group by operations
- Aggregations
- Joins
- CSV/JSON I/O
- Parallel processing
- Memory optimization
- Lazy evaluation

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 