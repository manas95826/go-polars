import numpy as np
from ._gopolars import DataFrame as _DataFrame
from enum import Enum

class AggType(Enum):
    SUM = 0
    MEAN = 1
    COUNT = 2
    MIN = 3
    MAX = 4

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

    def sort_values(self, by, ascending=True):
        """
        Sort DataFrame by the specified column.

        Parameters
        ----------
        by : str
            Name of the column to sort by
        ascending : bool, default True
            Sort ascending vs. descending

        Returns
        -------
        DataFrame
            A new sorted DataFrame
        """
        if not isinstance(by, str):
            raise TypeError("'by' must be a string")
        
        result = DataFrame()
        result._df = self._df.sort_by_column(by, ascending)
        return result

    def sort_index(self, ascending=True):
        """
        Sort DataFrame by the index.

        Parameters
        ----------
        ascending : bool, default True
            Sort ascending vs. descending

        Returns
        -------
        DataFrame
            A new sorted DataFrame
        """
        result = DataFrame()
        result._df = self._df.sort_by_index(ascending)
        return result

    def groupby(self, by):
        """
        Group DataFrame by one or more columns.

        Parameters
        ----------
        by : str or list of str
            Column name(s) to group by

        Returns
        -------
        GroupedDataFrame
            A grouped DataFrame
        """
        if isinstance(by, str):
            by = [by]
        elif not isinstance(by, list) or not all(isinstance(col, str) for col in by):
            raise TypeError("'by' must be a string or list of strings")

        return GroupedDataFrame(self._df.group_by(by))

class GroupedDataFrame:
    """
    A grouped DataFrame.
    """
    def __init__(self, grouped_df):
        self._grouped_df = grouped_df

    def agg(self, column, aggtype):
        """
        Aggregate using one or more operations.

        Parameters
        ----------
        column : str
            Column to aggregate
        aggtype : AggType
            Type of aggregation to perform

        Returns
        -------
        DataFrame
            A new DataFrame with the aggregated results
        """
        if not isinstance(column, str):
            raise TypeError("'column' must be a string")
        if not isinstance(aggtype, AggType):
            raise TypeError("'aggtype' must be an AggType")

        result = DataFrame()
        result._df = self._grouped_df.aggregate(column, aggtype.value)
        return result 