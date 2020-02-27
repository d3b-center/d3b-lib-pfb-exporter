"""
Export biomedical data from a relational database to an Avro file.

An Avro file stores the data schema as a JSON blob and the data in a
binary format

See https://avro.apache.org for details

In this case, the Avro file is called a PFB
(Portable Format for Bioinformatics) file because the data in the Avro file
conforms to the PFB schema, which is a graph structure suitable for capturing
relational data.

PFB file creations involves the following steps:

1. Create a PFB schema to represent the relational database
2. Transform the data from the relational database into the graph form
3. Add the PFB schema to the Avro file as an Avro schema
4. Add the transformed graph data to the Avro file as data records

Supported Databases:
- Any of the databases supported by SQLAlchemy since the SQLAlchemy ORM
is used to inspect the database and autogenerate the SQLAlchemy models
which are in turn used to create the PFB Schema.
"""
import os
import logging

from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PFB_FILE,
    DEFAULT_MODELS_PATH
)
from pfb_exporter.utils import setup_logger
from pfb_exporter.transform.sqla import SqlaTransformer


class PfbBuilder(object):

    def __init__(
        self,
        data_dir,
        db_conn_url=None,
        models_filepath=DEFAULT_MODELS_PATH,
        output_dir=DEFAULT_OUTPUT_DIR
    ):
        setup_logger(os.path.join(output_dir, 'logs'))
        self.logger = logging.getLogger(type(self).__name__)
        self.models_filepath = os.path.abspath(
            os.path.expanduser(models_filepath)
        )
        self.data_dir = os.path.abspath(os.path.expanduser(data_dir))
        self.output_dir = os.path.abspath(os.path.expanduser(output_dir))

        self.pfb_file = os.path.join(output_dir, DEFAULT_PFB_FILE)

        # Relational model to PFB Schema transformer
        self.transformer = SqlaTransformer(
            self.data_dir,
            self.models_filepath,
            self.output_dir,
            db_conn_url=db_conn_url
        )

    def export(self, output_to_pfb=True):
        """
        Entry point to create a PFB file containing data from a relational
        database

        - (Optional) Inspect DB and generate the SQLAlchemy models
        - Transform the models into a PFB Schema
        - Transform the data into PFB Entities
        - Create an Avro file with the PFB schema and Entities

        :param output_to_pfb: whether to complete the export after transforming
        the relational model to the PFB schema
        :type output_to_pfb: bool
        """
        try:
            # Transform relational model to PFB Schema
            pfb_schema = self.transformer.create_schema()

            # Transform relational data to PFB Entity JSON objects
            pfb_entities = self.transformer.create_entities()

            # Create the PFB file from the PFB Schema and data
            if output_to_pfb:
                self._build_pfb(pfb_schema, pfb_entities)

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info(f'❌ Export to PFB file {self.pfb_file} failed!')
            exit(1)
        else:
            self.logger.info(
                f'✅ Export to PFB file {self.pfb_file} succeeded!'
            )

    def _build_pfb(self, pfb_schema, pfb_entities):
        """
        Create a PFB file from a PFB Schema and PFB Entity JSON objects
        """
        # TODO
        # Add schema to temporary avro file
        # Add data records
        pass
