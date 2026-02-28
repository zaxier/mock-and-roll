"""Tests for bundled_core data module."""

import pandas as pd

from mock_and_roll.bundled_core.data import Dataset, DataModel


def test_dataset_creation():
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    ds = Dataset(name="test", data=df)
    assert ds.name == "test"
    assert len(ds.data) == 3
    assert ds.subdirectory is None


def test_dataset_get_file_path():
    df = pd.DataFrame({"id": [1]})
    ds = Dataset(name="users", data=df)
    assert ds.get_file_path("/base") == "/base/users"

    ds_sub = Dataset(name="users", data=df, subdirectory="raw")
    assert ds_sub.get_file_path("/base") == "/base/raw/users"


def test_datamodel_creation():
    df1 = pd.DataFrame({"id": [1]})
    df2 = pd.DataFrame({"id": [2]})
    dm = DataModel(
        datasets=[
            Dataset(name="users", data=df1),
            Dataset(name="orders", data=df2),
        ]
    )
    assert len(dm.datasets) == 2


def test_datamodel_get_dataset():
    df = pd.DataFrame({"id": [1]})
    dm = DataModel(datasets=[Dataset(name="users", data=df)])

    found = dm.get_dataset("users")
    assert found is not None
    assert found.name == "users"

    not_found = dm.get_dataset("nonexistent")
    assert not_found is None
