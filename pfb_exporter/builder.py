"""
Transform and export data from a relational database into a
PFB (Portable Format for Bioinformatics) file.

A PFB file is an Avro file with a particular Avro schema that represents a
relational database. We call this schema the PFB File Schema

See pfb_exporter.schema for more information on the PFB File Schema

The data in a PFB file contains a list of JSON objects called PFB Entity
objects. There are 2 types of PFB Entities. One (Metadata) captures
information about the relational database and the other (Table Row) captures
a row of data from a particular table in the database.

The data records in a PFB file are produced by transforming the original data
from a relational database into PFB Entity objects. Each PFB Entity object
conforms to its Avro schema.

PFB File Creation
-----------------

1. Create the Avro schemas PFB Entities and the PFB File
2. Transform the JSON objects representing rows of data from the relational
   database into PFB Entities
3. Add the Avro schemas to the PFB Avro file
4. Add the PFB Entities to the Avro file

PFB Schema Creation
-------------------
The PFB File schema is created from SQLAlchemy declarative base classes
in a file or directory. If the classes are not provided, they are generated
by inspecting the DB schema using the sqlacodegen library.

See https://github.com/agronholm/sqlacodegen

Supported Databases
-------------------
Theoretically, any of the databases supported by SQLAlchemy but this
has only been tested on a PostgreSQL database
"""

import os
import logging
from pprint import pformat, pprint

from fastavro import parse_schema

from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PFB_FILE,
    SQLA_MODELS_FILE,
    DEFAULT_AVRO_SCHEMA_NAMESPACE
)
from pfb_exporter.utils import setup_logger
from pfb_exporter.schema import PfbFileSchema
from pfb_exporter.sqla import SqlaModelBuilder


class PfbFileBuilder(object):

    def __init__(
        self,
        data_dir,
        db_conn_url=None,
        models_path=None,
        output_dir=DEFAULT_OUTPUT_DIR,
        namespace=DEFAULT_AVRO_SCHEMA_NAMESPACE
    ):
        """
        Build a PFB Avro file containing data from a biomedical relational
        database

        :param data_dir: A directory of JSON ND files. Each file is expected
        to contain JSON objects each of which contains row data from a
        database table. See yield_entities for details
        :type data_dir: str
        :param db_conn_url: The database connection URL.
        Example: postgresql://postgres:mypassword@127.0.0.1:5432/mydb
        :type db_conn_url: str
        :param models_path: path to where the SQLAlchemy models are or
        will be written if generated
        :type models_path: str
        :param output_dir: directory where all intermediate outputs and the
        PFB Avro file will be written.
        :type output_dir: str
        :param namespace: the Avro schema namespace
        :type namespace: str
        """

        setup_logger(os.path.join(output_dir, 'logs'))
        self.logger = logging.getLogger(type(self).__name__)
        self.data_dir = os.path.abspath(os.path.expanduser(data_dir))
        self.db_conn_url = db_conn_url
        self.models_path = models_path or os.path.join(
            self.output_dir, SQLA_MODELS_FILE
        )
        self.output_dir = os.path.abspath(os.path.expanduser(output_dir))
        self.namespace = namespace

        self.pfb_file = os.path.join(self.output_dir, DEFAULT_PFB_FILE)

    def build(self, write_pfb=True):
        """
        Build PFB entities and write to an Avro file

        Import (or generate and import) the ORM classes
        Create the PFB File Avro schema
        Create and yield the PFB Entities
        Write each PFB Entity as its yielded to the output Avro file,
        pfb_file
        """
        try:
            # Import the SQLAlchemy model classes from file
            self.model_builder = SqlaModelBuilder(
                self.models_path,
                self.output_dir,
                db_conn_url=self.db_conn_url
            )
            self.model_builder.create_and_import_models()

            if not (self.db_conn_url or
                    self.model_builder.imported_model_classes):
                raise RuntimeError(
                    'There are 0 models to generate the PFB file. You must '
                    'provide a DB connection URL that can be used to '
                    'connect to a database to generate the models or '
                    'provide a dir or file path to where the models reside'
                )

            # Build the PFB file Avro schema
            self.pfb_file_schema = PfbFileSchema(
                self.model_builder.orm_models_dict, self.output_dir,
                self.namespace
            )

            # Build the PFB file entities and write to the Avro file
            if write_pfb:
                self.logger.info(
                    f'✏️ Begin writing PFB Avro file to {self.pfb_file}'
                )
                if os.path.isfile(self.pfb_file):
                    os.remove(self.pfb_file)

                parsed_schema = parse_schema(self.pfb_file_schema.avro_schema)

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info(f'❌ Export to PFB file {self.pfb_file} failed!')
            exit(1)
        else:
            self.logger.info(
                f'✅ Export to PFB file {self.pfb_file} succeeded!'
            )
            pass
