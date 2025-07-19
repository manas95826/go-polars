import numpy as np
import go_polars as gp
import pytest

def test_create_dataframe():
    data = {
        'a': np.array([1, 2, 3], dtype=np.int64),
        'b': np.array([1.1, 2.2, 3.3], dtype=np.float64),
        'c': np.array([True, False, True], dtype=np.bool_)
    }
    df = gp.DataFrame.from_dict(data)
    assert df.shape == (3, 3)

def test_from_dict():
    data = {
        'a': np.array([1, 2, 3], dtype=np.int64),
        'b': np.array([1.1, 2.2, 3.3], dtype=np.float64),
        'c': np.array([True, False, True], dtype=np.bool_)
    }
    df = gp.DataFrame.from_dict(data)
    assert df.shape == (3, 3)

def test_invalid_dtype():
    data = {
        'a': np.array(['str1', 'str2'], dtype=str)
    }
    with pytest.raises(TypeError, match="Unsupported dtype"):
        gp.DataFrame.from_dict(data) 