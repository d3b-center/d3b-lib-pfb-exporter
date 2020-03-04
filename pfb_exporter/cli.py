"""
Entry point for the PFB Exporter
"""

import click


from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    SQLA_MODELS_FILE,
    DEFAULT_AVRO_SCHEMA_NAMESPACE
)
from pfb_exporter.builder import PfbFileBuilder

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    A CLI wrapper for running the pfb exporter
    """
    pass


def common_args_options(func):
    """
    Common click args and options
    """
    # The string to use as the Avro schema namespace
    func = click.option(
        '--namespace', '-n',
        help='The namespace (e.g. kidsfirst) to use in the Avro schema',
        show_default=True,
        default=DEFAULT_AVRO_SCHEMA_NAMESPACE)(func)

    # Output directory where PFB file, models, and logs get written
    func = click.option(
        '--output_dir', '-o',
        help='Path to the output directory',
        show_default=True,
        default=DEFAULT_OUTPUT_DIR,
        type=click.Path(exists=False, file_okay=False, dir_okay=True))(func)

    # Path to dir or file where models reside
    func = click.option(
        '--models_path', '-m',
        help=(
            'Path to a file or directory where SQLAlchemy models will be '
            'written to (if generated from database) or read from.'
            'If a path is not provided, the default: '
            f'<output directory>/{SQLA_MODELS_FILE} will be used'
        ),
        type=click.Path(exists=False, file_okay=True, dir_okay=True))(func)

    # Db connection url
    func = click.option(
        '--database_url', '-d',
        default=None,
        help=(
            'The connection URL to the database from which SQLAlchemy models '
            'will be generated. See help for --models_path for details '
            'on where models will be written to'
        ))(func)

    func = click.argument(
        'data_dir',
        type=click.Path(exists=True, file_okay=True, dir_okay=True))(func)

    return func


@click.command()
@common_args_options
def export(
    data_dir, database_url, models_path, output_dir, namespace
):
    """
    Transform and export data from a relational database into a
    PFB (Portable Format for Bioinformatics) file.

    A PFB file is special kind of Avro file, suitable for capturing and
    reconstructing relational data. Read Background for more information.

    \b
    Arguments:
        \b
        data_dir - Path to directory containing the JSON payloads which
        conform to the SQLAlchemy models.
    """
    PfbFileBuilder(
        data_dir, database_url, models_path, output_dir, namespace
    ).build()


@click.command('create_schema')
@common_args_options
def create_schema(
    data_dir, database_url, models_path, output_dir, namespace
):
    """
    Generate a PFB Schema from the database. The PFB Schema is required to
    create the PFB file.

    \b
    Arguments:
        \b
        data_dir - Path to directory containing the JSON payloads which
        conform to the sqlalchemy models.
    """

    PfbFileBuilder(
        '', database_url, models_path, output_dir, namespace
    ).build(write_pfb=False)


cli.add_command(export)
cli.add_command(create_schema)
