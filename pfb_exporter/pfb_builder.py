"""
Transform and export data from a relational database into a
PFB (Portable Format for Bioinformatics) file.

A PFB file is an Avro file with a particular schema that represents a graph
structure (adjacency list). We call this schema the
[PFB Schema](https://github.com/uc-cdis/pypfb/tree/master/doc)

The graph structure consists of a list of JSON objects called PFB Entity
objects. Each PFB Entity conforms to the PFB Schema. A PFB Entity has
attributes + values, relations to other PFB Entities, and ontology references.
The ontology references can be attached to the PFB Entity and also to each
attribute of the PFB Entity.

The data records in a PFB file are produced by transforming the original data
from a relational database into PFB Entity objects.

PFB file creations involves the following steps:

1. Create a PFB schema to represent the relational database
2. Transform the data from the relational database into PFB Entities
3. Add the PFB schema to the Avro file
4. Add the PFB Entities to the Avro file

Supported Databases:
- Any of the databases supported by SQLAlchemy since the SQLAlchemy ORM
is used to inspect the database and autogenerate the SQLAlchemy models
which are in turn used to create the PFB Schema.
"""
import os
import logging

from fastavro import writer, parse_schema

from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PFB_FILE,
    DEFAULT_MODELS_PATH
)
from pfb_exporter.utils import setup_logger
from pfb_exporter.builder.sqla import SqlaModelBuilder
from pfb_exporter.builder.pfb_schema import PfbSchemaBuilder
from pfb_exporter.builder.pfb_entity import PfbEntityBuilder


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
        self.data_dir = os.path.abspath(os.path.expanduser(data_dir))
        self.db_conn_url = db_conn_url
        self.models_filepath = os.path.abspath(
            os.path.expanduser(models_filepath)
        )
        self.output_dir = os.path.abspath(os.path.expanduser(output_dir))
        self.pfb_file = os.path.join(output_dir, DEFAULT_PFB_FILE)

    def export(self, output_to_pfb=True):
        """
        Entry point to create a PFB file

        - (Optional) Inspect DB and generate the SQLAlchemy models
        - Transform the models into a PFB Schema
        - Transform the data into PFB Entities
        - Create an Avro file with the PFB Schema and Entities

        :param output_to_pfb: whether to complete the export after transforming
        the relational model to the PFB schema
        :type output_to_pfb: bool
        """
        try:
            # Import SQLAlchemy model classes
            model_builder = SqlaModelBuilder(
                self.models_filepath,
                self.output_dir,
                db_conn_url=self.db_conn_url
            )
            model_builder.create_and_import_models()

            # Transform relational model to PFB Schema JSON object
            schema_builder = PfbSchemaBuilder(
                model_builder.imported_models, self.output_dir
            )
            pfb_schema = schema_builder.create()

            # Transform relational data to PFB Entity JSON objects
            pfb_entities = PfbEntityBuilder(
                schema_builder.relational_model, self.output_dir
            ).create()

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
        pass
        # parsed_schema = parse_schema(pfb_schema)
        #
        # with open(self.pfb_file, 'a+b') as avro_file:
        #     for ent in pfb_entities:
        #         writer(avro_file, parsed_schema, ent)
