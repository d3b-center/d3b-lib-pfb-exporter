import os

from conftest import TEST_DATA_DIR
from click.testing import CliRunner

from pfb_exporter import cli

OUTPUT_DIR = os.path.join(TEST_DATA_DIR, 'pfb_export')
DATA_DIR = os.path.join(TEST_DATA_DIR, 'input')


def test_export():
    """
    Test pfb_exporter.cli.export
    """
    model_dir = DATA_DIR

    runner = CliRunner()
    result = runner.invoke(
        cli.export,
        [DATA_DIR, '-m', model_dir, '-o', OUTPUT_DIR]
    )

    assert result.exit_code == 0


def test_create_schema():
    """
    Test pfb_exporter.cli.create_schema
    """
    OUTPUT_DIR = os.path.join(TEST_DATA_DIR, 'pfb_export')
    model_dir = DATA_DIR

    runner = CliRunner()
    result = runner.invoke(
        cli.create_schema,
        [DATA_DIR, '-m', model_dir, '-o', OUTPUT_DIR]
    )

    assert result.exit_code == 0
