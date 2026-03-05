import pytest

from mock_and_roll.templates import choose_template, slugify, suggest_spec


@pytest.mark.unit
def test_choose_template_sales():
    template_name, columns = choose_template("Create ecommerce order and customer sales data")
    assert template_name == "sales"
    assert len(columns) > 3


@pytest.mark.unit
def test_slugify_defaults_for_empty_value():
    assert slugify("!!!") == "sample_dataset"


@pytest.mark.unit
def test_suggest_spec_uses_inputs():
    spec = suggest_spec(
        description="Generate payment transaction examples for fraud analytics",
        catalog="dev",
        schema="sandbox",
        table="fraud_txns",
        rows=250,
    )
    assert spec.catalog == "dev"
    assert spec.schema_name == "sandbox"
    assert spec.table == "fraud_txns"
    assert spec.rows == 250
    assert len(spec.columns) > 4
