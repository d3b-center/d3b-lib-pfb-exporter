"""
Entry point for the PFB Exporter
"""

import click


from pfb_exporter.config import (
    DEFAULT_TRANFORM_MOD,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_MODELS_PATH
)
from pfb_exporter.builder import PfbBuilder

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

    # Path to Python transform module
    func = click.option(
        '--transform_module', '-t',
        help='Path to the Python module used to transform from a '
        'relational model to the Gen3 data dictionary',
        show_default=True,
        default=DEFAULT_TRANFORM_MOD,
        type=click.Path(exists=True, file_okay=True, dir_okay=False))(func)

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
    data_dir, database_url, models_filepath, transform_module, output_dir
):
    """
    Export relational data to PFB (Portable Bioinformatics Format) file

    \b
    Arguments:
        \b
        data_dir - Path to directory containing the JSON payloads which
        conform to the SQLAlchemy models.
    """
    PfbBuilder(
        data_dir, database_url, models_filepath, transform_module, output_dir,
    ).export()


@click.command('create_schema')
@common_args_options
def create_schema(
    data_dir, database_url, models_filepath, transform_module, output_dir
):
    """
    Transform the relational model into a PFB Schema, which is required to
    create the PFB file

    \b
    Arguments:
        \b
        data_dir - Path to directory containing the JSON payloads which
        conform to the sqlalchemy models.
    """

    PfbBuilder(
        '', database_url, models_filepath, transform_module, output_dir
    ).export(output_to_pfb=False)


cli.add_command(export)
cli.add_command(create_schema)
