import numpy as np
import gopolars as gp

# Create a DataFrame from a dictionary
data = {
    'a': np.array([1, 2, 3], dtype=np.int64),
    'b': np.array([1.1, 2.2, 3.3], dtype=np.float64),
    'c': np.array([True, False, True], dtype=np.bool_)
}

df = gp.DataFrame.from_dict(data)
print("DataFrame shape:", df.shape) 