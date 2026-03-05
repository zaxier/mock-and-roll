import pytest

from mock_and_roll.generator import DatasetGenerator
from mock_and_roll.spec import ColumnSpec, DatasetSpec


def _build_spec() -> DatasetSpec:
    return DatasetSpec(
        name="unit_test_dataset",
        description="Unit test dataset",
        catalog="dev",
        schema="sandbox",
        table="unit_test_dataset",
        rows=5,
        columns=[
            ColumnSpec(name="id", type="int", generator="sequence.int", args={"start": 10, "step": 2}),
            ColumnSpec(name="email", type="string", generator="person.email"),
            ColumnSpec(name="amount", type="double", generator="finance.price", args={"minimum": 1, "maximum": 5}),
            ColumnSpec(name="is_active", type="boolean", generator="choice.choice", args={"items": [True, False]}),
        ],
    )


@pytest.mark.unit
def test_generator_outputs_expected_shape_and_columns():
    spec = _build_spec()
    generator = DatasetGenerator(seed=42)
    frame = generator.generate(spec)

    assert len(frame) == 5
    assert list(frame.columns) == ["id", "email", "amount", "is_active"]
    assert frame["id"].tolist() == [10, 12, 14, 16, 18]


@pytest.mark.unit
def test_generator_supports_row_override():
    spec = _build_spec()
    generator = DatasetGenerator(seed=1)
    frame = generator.generate(spec, rows=2)
    assert len(frame) == 2

