"""
Entry point for the PFB Exporter
"""
import os

import click


from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    SQLA_MODELS_FILE,
    DEFAULT_AVRO_SCHEMA_NAMESPACE
)
from pfb_exporter.builder import PfbFileBuilder
from pfb_exporter.db import DbUtils

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    A CLI wrapper for running the pfb exporter
    """
    pass

# Common arguments and options


database_url_opt = click.option(
    '--database_url', '-d',
    default=None,
    help=(
        'The connection URL to the database from which SQLAlchemy models '
        'will be generated and written to file. See --models_path for details '
        'on where models are written.'
    )
)
pfb_export_output_dir_opt = click.option(
    '--output_dir', '-o',
    help='Path to the PFB export output directory',
    show_default=True,
    default=DEFAULT_OUTPUT_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True)
)
model_paths_opt = click.option(
    '--models_path', '-m',
    help=(
        'Path to a file or directory where SQLAlchemy models will be '
        'written to (if generated from database) or read from.'
        'If a path is not provided, the default: '
        f'<output directory>/{SQLA_MODELS_FILE} will be used'
    ),
    type=click.Path(exists=False, file_okay=True, dir_okay=True)
)
namespace_opt = click.option(
    '--namespace', '-n',
    help='The namespace (e.g. kidsfirst) to use in the Avro schema',
    show_default=True,
    default=DEFAULT_AVRO_SCHEMA_NAMESPACE
)

sql_file_opt = click.option(
    '--sql_file', '-s',
    help=(
        'This option must be provided with the --table_name option. '
        'Path to a .sql file containing the SQL query for selecting data to '
        'stream from database. If this option is not provided then all data '
        'from the database will be selected.'
    ),
    type=click.Path(exists=False, file_okay=True, dir_okay=False)
)

table_name_opt = click.option(
    '--table_name', '-t',
    help=(
        'This option must be provided with the --sql_file option. It is the '
        'name of the table that all results from the SQL query in --sql_file '
        'conform to. '
    )
)


@click.command()
@namespace_opt
@pfb_export_output_dir_opt
@model_paths_opt
@database_url_opt
@click.argument(
    'data_dir',
    type=click.Path(exists=True, file_okay=True, dir_okay=True))
def export(
    data_dir, database_url, models_path, output_dir, namespace
):
    """
    Transform and export data from a relational database into a
    PFB (Portable Format for Bioinformatics) file.

    A PFB file is special kind of Avro file, suitable for capturing and
    reconstructing relational data. Read pfb_exporter.builder for more info

    \b

    Arguments:
    \b

    data_dir:

    Data from the JSON ND files the data_dir directory will be supplied to the
    PFB file builder. Each JSON ND file MUST follow this format:

            "biospecimen"
            { biospecimen JSON object 0 }
                        ...
            { biospecimen JSON object N }

    The first JSON object in the file must be the name of the table that
    subsequent JSON objects conform to. Any JSON object after the first one
    in the file captures the row data conforming to that table's schema.
    """

    PfbFileBuilder(
        data_dir,
        db_conn_url=database_url,
        models_path=models_path,
        output_dir=output_dir,
        namespace=namespace
    ).build()


@click.command('db_export')
@namespace_opt
@pfb_export_output_dir_opt
@sql_file_opt
@table_name_opt
@click.argument('database_url')
def db_export(
    database_url, table_name, sql_file, output_dir, namespace
):
    """
    Does the same thing as export command except that the input data is
    streamed from the database to the PFB file builder.

    The data to be streamed from the database can be selected via a SQL query
    in `sql_file`. If the `sql_file` option is used, the `table_name` option
    must also be provided.

    The SQL query in `sql_file must return rows that conform to the table's
    (specified by `table_name`) schema. If a query is not provided, then all
    rows from `table_name` table will be selected.

    \b
    Arguments:
        \b
        dastabase_url - The connection URL to the database from which
        data will be streamed
    """
    PfbFileBuilder(
        '', database_url, '', output_dir, namespace
    ).build(table_name=table_name, sql_file=sql_file)


@click.command('create_schema')
@namespace_opt
@pfb_export_output_dir_opt
@model_paths_opt
@database_url_opt
def create_schema(
    database_url, models_path, output_dir, namespace
):
    """
    Generate a PFB Schema from the database. The PFB Schema is required to
    create the PFB file.
    """

    PfbFileBuilder(
        '', database_url, models_path, output_dir, namespace
    ).create_pfb_schema()


@click.command('download')
@click.option(
    '--output_dir', '-o',
    help=(
        'Path to the directory where JSON ND file will be written. '
        'File will be named <table_name>.ndjson'
    ),
    show_default=True,
    default=os.path.join(DEFAULT_OUTPUT_DIR, 'ndjson'),
    type=click.Path(exists=False, file_okay=False, dir_okay=True)
)
@sql_file_opt
@click.argument('table_name')
@click.argument('database_url')
def download(table_name, database_url, sql_file, output_dir):
    """
    Execute a SQL query to download data from the database. Write the rows
    to JSON objects in a JSON ND file.

    The file will follow this format:

        "biospecimen"
        { biospecimen JSON object 0 }
                    ...
        { biospecimen JSON object N }

    The first JSON object in the file will be the name of the table that
    subsequent JSON objects conform to. Any JSON object after the first one
    in the file captures the row data conforming to that table's schema.

    \b
    Arguments:
        \b
        table_name -  The name of the table that all results from the SQL
        query conform to. This will also be used to name the JSON ND file
        containing the results
        (e.g. table_name=biospecimen, JSON ND file: biospecimen.ndjson)

        \b
        dastabase_url - The connection URL to the database from which
        data will be downloaded
    """
    DbUtils(database_url, output_dir).table_rows_to_ndjson(
        table_name, sql_file_path=sql_file
    )


cli.add_command(export)
cli.add_command(db_export)
cli.add_command(create_schema)
cli.add_command(download)
