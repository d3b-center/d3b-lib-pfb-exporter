import os
from collections import defaultdict
from pprint import pprint

from conftest import TEST_DATA_DIR
from click.testing import CliRunner
from fastavro import reader as avro_reader
from deepdiff import DeepDiff

from pfb_exporter import cli
from pfb_exporter.builder import PfbFileBuilder

OUTPUT_DIR = os.path.join(TEST_DATA_DIR, 'pfb_export')
DATA_DIR = os.path.join(TEST_DATA_DIR, 'input')
MODELS_PATH = os.path.join(TEST_DATA_DIR, 'models.py')


def test_export():
    """
    Test pfb_exporter.cli.export
    """
    runner = CliRunner()
    result = runner.invoke(
        cli.export,
        [DATA_DIR, '-m', MODELS_PATH, '-o', OUTPUT_DIR]
    )

    assert result.exit_code == 0


def test_create_schema():
    """
    Test pfb_exporter.cli.create_schema
    """
    runner = CliRunner()
    result = runner.invoke(
        cli.create_schema,
        [DATA_DIR, '-m', MODELS_PATH, '-o', OUTPUT_DIR]
    )

    assert result.exit_code == 0


def test_validate_pfb_build():
    """
    Validate the PFB file

    Source data -> Write PFB File -> Read PFB file -> Compare with source data
    """
    # Create PFB file
    builder = PfbFileBuilder(
        DATA_DIR,
        models_path=MODELS_PATH,
        output_dir=OUTPUT_DIR
    )
    builder.build()

    # Create pfb entities from source data
    source_records = defaultdict(lambda: defaultdict(dict))
    for record in builder.yield_entities():
        source_records[record['name']][record['id']] = record

    # Read PFB Entity JSON objects from PFB file
    with open(builder.pfb_file, 'rb') as fo:
        records_from_file = [record for record in avro_reader(fo)]

    # Compare with original
    for i, file_record in enumerate(records_from_file):
        # if file_record['name'] == 'Metadata':
        #     continue
        print(f'Testing {file_record["name"]}')
        source_record = source_records[file_record['name']][file_record['id']]
        source_obj = source_record['object'][1]
        file_obj = file_record['object']
        diff = DeepDiff(source_obj, file_obj, ignore_order=True)
        assert not diff

    # Check output files
    assert os.path.isfile(builder.model_builder.models_path)
    assert os.path.isfile(builder.model_builder.orm_models_file)
    assert os.path.isfile(builder.pfb_file_schema.avro_schema_file)
    assert os.path.isfile(builder.metadata_file)
