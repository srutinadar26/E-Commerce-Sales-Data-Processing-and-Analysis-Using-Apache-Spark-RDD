# src/mock_spark.py
"""
Pure-Python emulation of PySpark RDDs.

Used as a fallback on Windows + Python 3.12+ where PySpark's Python worker
daemon crashes with WinError 10038 / WSAENOTSOCK.

All 13 RDD operations required by the project are implemented here so that
the full pipeline runs and all unit tests pass, even without a working
Java/Py4J worker bridge.
"""

from __future__ import annotations
from typing import Any, Callable, Iterable, Iterator, List, Tuple


class _ResultIterable:
    """Mimics pyspark.resultiterable.ResultIterable returned by groupByKey."""
    def __init__(self, data: list):
        self._data = data

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


class MockRDD:
    """Minimal RDD emulation supporting all operations used in this project."""

    def __init__(self, data: Iterable):
        self._data: list = list(data)

    # ── Transformations ──────────────────────────────────────────────────────

    def map(self, f: Callable) -> "MockRDD":
        return MockRDD(f(x) for x in self._data)

    def filter(self, f: Callable) -> "MockRDD":
        return MockRDD(x for x in self._data if f(x))

    def flatMap(self, f: Callable) -> "MockRDD":
        return MockRDD(item for x in self._data for item in f(x))

    def distinct(self) -> "MockRDD":
        seen, result = set(), []
        for x in self._data:
            key = x if not isinstance(x, dict) else id(x)
            try:
                if x not in seen:
                    seen.add(x)
                    result.append(x)
            except TypeError:
                result.append(x)
        return MockRDD(result)

    def union(self, other: "MockRDD") -> "MockRDD":
        return MockRDD(self._data + other._data)

    def reduceByKey(self, f: Callable) -> "MockRDD":
        d: dict = {}
        for k, v in self._data:
            d[k] = f(d[k], v) if k in d else v
        return MockRDD(d.items())

    def groupByKey(self) -> "MockRDD":
        d: dict = {}
        for k, v in self._data:
            d.setdefault(k, []).append(v)
        return MockRDD((k, _ResultIterable(vs)) for k, vs in d.items())

    def mapValues(self, f: Callable) -> "MockRDD":
        return MockRDD((k, f(v)) for k, v in self._data)

    def sortByKey(self, ascending: bool = True) -> "MockRDD":
        return MockRDD(sorted(self._data, key=lambda x: x[0], reverse=not ascending))

    def keys(self) -> "MockRDD":
        return MockRDD(k for k, _ in self._data)

    def values(self) -> "MockRDD":
        return MockRDD(v for _, v in self._data)

    def persist(self, storageLevel=None) -> "MockRDD":
        return self

    def unpersist(self) -> "MockRDD":
        return self

    def cache(self) -> "MockRDD":
        return self

    def repartition(self, n: int) -> "MockRDD":
        return self

    # ── Actions ──────────────────────────────────────────────────────────────

    def count(self) -> int:
        return len(self._data)

    def collect(self) -> list:
        return list(self._data)

    def take(self, n: int) -> list:
        return self._data[:n]

    def first(self) -> Any:
        return self._data[0]

    def takeOrdered(self, n: int, key: Callable = None) -> list:
        return sorted(self._data, key=key)[:n] if key else sorted(self._data)[:n]

    def reduce(self, f: Callable) -> Any:
        from functools import reduce
        return reduce(f, self._data)

    def fold(self, zero: Any, op: Callable) -> Any:
        from functools import reduce
        return reduce(op, self._data, zero)

    def sum(self) -> float:
        return sum(self._data)

    def max(self) -> Any:
        return max(self._data)

    def min(self) -> Any:
        return min(self._data)

    def getNumPartitions(self) -> int:
        return 4

    def __repr__(self) -> str:
        return f"MockRDD[{len(self._data)} elements]"


class MockSparkContext:
    """Minimal SparkContext emulation."""

    def __init__(self):
        self.defaultParallelism = 4

    def textFile(self, path: str, minPartitions: int = 1) -> MockRDD:
        import os
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(path, "r", encoding="utf-8") as fh:
            lines = [line.rstrip("\n") for line in fh]
        return MockRDD(lines)

    def parallelize(self, data: Iterable, numSlices: int = None) -> MockRDD:
        return MockRDD(data)

    def stop(self):
        pass

    def setLogLevel(self, level: str):
        pass


class MockSparkSession:
    """
    Minimal SparkSession emulation.

    Returned by ``get_spark_session()`` on Windows + Python 3.12+ so that
    the full pipeline and all unit tests pass without a real JVM.

    NOTE: The real Spark Web UI at localhost:4040 is NOT available when using
    this mock.  Start the pipeline via ``run.ps1`` with Python ≤ 3.11 to get
    the live UI.
    """

    def __init__(self):
        self.sparkContext = MockSparkContext()

    def stop(self):
        pass

    # Convenience proxy so code that references ``spark.sparkContext`` works.
    @property
    def sc(self) -> MockSparkContext:
        return self.sparkContext
