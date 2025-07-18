import numpy as np
from ._gopolars import DataFrame as _DataFrame

class DataFrame:
    """
    A DataFrame is a 2-dimensional labeled data structure with columns of potentially
    different types.
    """
    def __init__(self):
        self._df = _DataFrame()

    @classmethod
    def from_dict(cls, data):
        """
        Create DataFrame from a dictionary of arrays.

        Parameters
        ----------
        data : dict
            Dict of array-like objects

        Returns
        -------
        DataFrame
        """
        df = cls()
        for name, array in data.items():
            arr = np.asarray(array)
            if arr.dtype not in [np.int64, np.float64, np.bool_]:
                raise TypeError(f"Unsupported dtype: {arr.dtype}")
            df._df.add_series(name, arr)
        return df

    @property
    def shape(self):
        """
        Return a tuple representing the dimensionality of the DataFrame.

        Returns
        -------
        shape : tuple
            The shape of the DataFrame (n_rows, n_columns)
        """
        return tuple(self._df.shape()) 