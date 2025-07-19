"""Python interface for go-polars."""

from ._go_polars import DataFrame as _DataFrame
from enum import Enum
import numpy as np
import ctypes

class AggType(Enum):
    SUM = 0
    MEAN = 1
    COUNT = 2
    MIN = 3
    MAX = 4

class Series:
    """
    A Series is a one-dimensional labeled array.
    """
    def __init__(self, df, name):
        self._df = df
        self._name = name
        self._data = df._df.get_series(name)

    def __gt__(self, other):
        if isinstance(other, (int, float)):
            return np.array(self._data > other)
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, (int, float)):
            return np.array(self._data < other)
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, (int, float)):
            return np.array(self._data >= other)
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, (int, float)):
            return np.array(self._data <= other)
        return NotImplemented

    def __eq__(self, other):
        if isinstance(other, (int, float, bool)):
            return np.array(self._data == other)
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, (int, float, bool)):
            return np.array(self._data != other)
        return NotImplemented

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

    def head(self, n=5):
        """
        Return the first n rows of the DataFrame.

        Parameters
        ----------
        n : int, default 5
            Number of rows to return

        Returns
        -------
        DataFrame
            First n rows of the DataFrame
        """
        result = DataFrame()
        result._df = self._df.head(n)
        if result._df is None:
            raise RuntimeError("Failed to get head of DataFrame")
        return result

    def describe(self):
        """
        Generate descriptive statistics.

        Returns
        -------
        DataFrame
            Descriptive statistics including count, mean, std, min, 25%, 50%, 75%, max
        """
        result = DataFrame()
        stats = {}
        
        for col in self.columns():
            series = self._df.get_series(col)
            if isinstance(series, (np.ndarray)):
                if series.dtype == np.bool_:
                    # Convert boolean to int64 for statistics
                    series = series.astype(np.int64)
                stats[col] = {
                    'count': len(series),
                    'mean': np.mean(series),
                    'std': np.std(series),
                    'min': np.min(series),
                    '25%': np.percentile(series, 25),
                    '50%': np.percentile(series, 50),
                    '75%': np.percentile(series, 75),
                    'max': np.max(series)
                }
        
        # Convert stats to DataFrame format
        index = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']
        data = {}
        for stat in index:
            data[stat] = np.array([stats[col][stat] for col in stats.keys()], dtype=np.float64)
        
        # Create DataFrame manually
        result = DataFrame()
        for stat, values in data.items():
            result._df.add_series(stat, values)
        return result

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
        
        sorted_df = self._df.sort_by_column(by, ascending)
        return self.__class__._from_internal(sorted_df)

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
        sorted_df = self._df.sort_by_index(ascending)
        return self.__class__._from_internal(sorted_df)

    @classmethod
    def _from_internal(cls, internal_df):
        obj = cls.__new__(cls)
        obj._df = internal_df
        return obj

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

        grouped_df = self._df.group_by(by)
        if grouped_df is None:
            raise RuntimeError("Failed to group DataFrame")
        return GroupedDataFrame(grouped_df)

    def columns(self):
        """
        Get the column names of the DataFrame.

        Returns
        -------
        list
            List of column names
        """
        num_cols = self._df.get_column_count()
        if num_cols <= 0:
            return []

        cols = []
        for i in range(num_cols):
            col = self._df.get_column(i)
            if col is None:
                raise RuntimeError("Failed to get column name")
            cols.append(col)
        return cols

    def __getitem__(self, key):
        """
        Get a column or filter the DataFrame.

        Parameters
        ----------
        key : str or array-like
            If str, get the column with that name.
            If array-like, filter the DataFrame using the boolean mask.

        Returns
        -------
        Series or DataFrame
            If key is str, returns a Series.
            If key is array-like, returns a filtered DataFrame.
        """
        if isinstance(key, str):
            return Series(self, key)
        elif isinstance(key, np.ndarray) and key.dtype == np.bool_:
            # Filter DataFrame using boolean mask
            result = DataFrame()
            for col in self.columns():
                series = self._df.get_series(col)
                if series is None:
                    raise RuntimeError("Failed to get series data")
                filtered = series[key]
                result._df.add_series(col, filtered)
            return result
        else:
            raise TypeError("Invalid key type")

    def __str__(self):
        """
        Return a string representation of the DataFrame.
        """
        # Build string representation manually
        cols = []
        num_cols = self._df.get_column_count()
        if num_cols <= 0:
            return "Empty DataFrame"

        # Get column names
        for i in range(num_cols):
            col = self._df.get_column(i)
            if col is None:
                return "Error getting column names"
            cols.append(col)

        # Get shape
        rows, _ = self.shape

        # Build header
        result = "\t".join(cols) + "\n"

        # Build data rows
        for i in range(rows):
            row = []
            for col in cols:
                series = self._df.get_series(col)
                if series is None:
                    return "Error getting series data"
                if series.dtype == np.bool_:
                    row.append(str(bool(series[i])).lower())
                elif series.dtype == np.float64:
                    row.append(f"{series[i]:.6g}")
                else:
                    row.append(str(series[i]))
            result += "\t".join(row) + "\n"

        return result

    def __repr__(self):
        """
        Return a string representation of the DataFrame.
        """
        return self.__str__()

class GroupedDataFrame:
    """
    A grouped DataFrame.
    """
    def __init__(self, grouped_df):
        if grouped_df is None:
            raise ValueError("Cannot create GroupedDataFrame from None")
        self._df = grouped_df

    def agg(self, aggs):
        """
        Aggregate using one or more operations.

        Parameters
        ----------
        aggs : dict
            Column to aggregate mapping to aggregation function name
            Example: {'A': 'sum', 'B': 'mean'}

        Returns
        -------
        DataFrame
            A new DataFrame with the aggregated results
        """
        if not isinstance(aggs, dict):
            raise TypeError("'aggs' must be a dictionary")

        result = DataFrame()
        
        # Map string aggregation names to AggType enum
        agg_map = {
            'sum': AggType.SUM,
            'mean': AggType.MEAN,
            'count': AggType.COUNT,
            'min': AggType.MIN,
            'max': AggType.MAX
        }

        # Process each aggregation
        for col, agg_name in aggs.items():
            if not isinstance(col, str):
                raise TypeError(f"Column name must be string, got {type(col)}")
            if not isinstance(agg_name, str):
                raise TypeError(f"Aggregation type must be string, got {type(agg_name)}")
            
            agg_type = agg_map.get(agg_name.lower())
            if agg_type is None:
                raise ValueError(f"Unsupported aggregation type: {agg_name}")
            
            result._df = self._df.aggregate(col, agg_type.value)
            if result._df is None:
                raise RuntimeError(f"Failed to aggregate column {col}")
        
        return result

    def __str__(self):
        """
        Return a string representation of the DataFrame.
        """
        # Build string representation manually
        cols = []
        num_cols = self._df.get_column_count()
        if num_cols <= 0:
            return "Empty DataFrame"

        # Get column names
        for i in range(num_cols):
            col = self._df.get_column(i)
            if col is None:
                return "Error getting column names"
            cols.append(col)

        # Get shape
        rows, _ = self._df.shape()

        # Build header
        result = "\t".join(cols) + "\n"

        # Build data rows
        for i in range(rows):
            row = []
            for col in cols:
                series = self._df.get_series(col)
                if series is None:
                    return "Error getting series data"
                if series.dtype == np.bool_:
                    row.append(str(bool(series[i])).lower())
                elif series.dtype == np.float64:
                    row.append(f"{series[i]:.6g}")
                else:
                    row.append(str(series[i]))
            result += "\t".join(row) + "\n"

        return result

    def __repr__(self):
        """
        Return a string representation of the DataFrame.
        """
        return self.__str__()

__all__ = ['DataFrame', 'AggType'] 