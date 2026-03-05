import pytest

from mock_and_roll import cli
from mock_and_roll.spec import ColumnSpec, DatasetSpec


@pytest.mark.unit
def test_build_parser_has_expected_commands():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "suggest",
            "--description",
            "sales test data",
            "--catalog",
            "dev",
            "--schema",
            "sandbox",
        ]
    )
    assert args.command == "suggest"


@pytest.mark.unit
def test_build_parser_supports_suggest_model():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "suggest-model",
            "--description",
            "Atlassian data lake with jira and confluence",
            "--catalog",
            "dev",
            "--schema",
            "sandbox",
        ]
    )
    assert args.command == "suggest-model"


@pytest.mark.unit
def test_run_create_invokes_writer(monkeypatch, tmp_path):
    spec = DatasetSpec(
        name="sample",
        description="sample",
        catalog="dev",
        schema="sandbox",
        table="sample_table",
        rows=2,
        columns=[ColumnSpec(name="id", type="int", generator="sequence.int", args={"start": 1})],
    )
    spec_file = tmp_path / "spec.yml"
    spec_file.write_text(
        "name: sample\n"
        "description: sample\n"
        "catalog: dev\n"
        "schema: sandbox\n"
        "table: sample_table\n"
        "rows: 2\n"
        "columns:\n"
        "  - name: id\n"
        "    type: int\n"
        "    generator: sequence.int\n"
        "    args:\n"
        "      start: 1\n",
        encoding="utf-8",
    )

    calls = {}

    def fake_generate(self, spec_arg, rows=None):
        calls["generated"] = True
        assert spec_arg.table == spec.table
        return __import__("pandas").DataFrame({"id": [1, 2]})

    def fake_write(**kwargs):
        calls["written"] = kwargs["spec"].table
        return "dev.sandbox.sample_table"

    monkeypatch.setattr("mock_and_roll.cli.DatasetGenerator.generate", fake_generate)
    monkeypatch.setattr("mock_and_roll.cli.write_dataset_to_delta", fake_write)

    parser = cli.build_parser()
    args = parser.parse_args(["create", "--spec", str(spec_file)])
    code = cli.run_create(args)

    assert code == 0
    assert calls["generated"] is True
    assert calls["written"] == "sample_table"
