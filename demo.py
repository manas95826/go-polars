import numpy as np
import go_polars as gp

def main():
    # Create sample data with different types
    data = {
        'integers': np.array([1, 2, 3, 4, 5], dtype=np.int64),
        'floats': np.array([1.1, 2.2, 3.3, 4.4, 5.5], dtype=np.float64),
        'booleans': np.array([True, False, True, False, True], dtype=np.bool_)
    }
    
    # Create DataFrame from dictionary
    df = gp.DataFrame.from_dict(data)
    
    # Print basic information
    print("\nDataFrame Shape:")
    print(df.shape)
    
    # Note: The library is focused on high-performance operations
    # and currently supports basic DataFrame creation with numpy arrays
    # of types int64, float64, and bool
    
if __name__ == "__main__":
    main() 