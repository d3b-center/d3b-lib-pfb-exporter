"""
Entry point for the PFB Exporter
"""

import click


from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_MODELS_PATH
)
from pfb_exporter.pfb_builder import PfbBuilder

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
    # Output directory where PFB file, models, and logs get written
    func = click.option(
        '--output_dir', '-o',
        help='Path to the output directory',
        show_default=True,
        default=DEFAULT_OUTPUT_DIR,
        type=click.Path(exists=False, file_okay=False, dir_okay=True))(func)

    # Path to dir or file where models reside
    func = click.option(
        '--models_filepath', '-m',
        show_default=True,
        default=DEFAULT_MODELS_PATH,
        type=click.Path(exists=False, file_okay=True, dir_okay=True))(func)

    # Db connection url
    func = click.option(
        '--database_url', '-d',
        default=None,
        help='The connection URL to the database from which SQLAlchemy models '
        'will be generated')(func)

    func = click.argument(
        'data_dir',
        type=click.Path(exists=True, file_okay=True, dir_okay=True))(func)

    return func


@click.command()
@common_args_options
def export(
    data_dir, database_url, models_filepath, output_dir
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
    PfbBuilder(
        data_dir, database_url, models_filepath, output_dir,
    ).export()


@click.command('create_schema')
@common_args_options
def create_schema(
    data_dir, database_url, models_filepath, output_dir
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

    PfbBuilder(
        '', database_url, models_filepath, output_dir
    ).export(output_to_pfb=False)


cli.add_command(export)
cli.add_command(create_schema)
