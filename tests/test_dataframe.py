import numpy as np
import pytest
import gopolars as gp

def test_create_dataframe():
    df = gp.DataFrame()
    assert df.shape == (0, 0)

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