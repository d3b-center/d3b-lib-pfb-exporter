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

1. Create the Avro schemas for PFB Entity types and the PFB File
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
import json
import jsonlines
from itertools import chain
from pprint import pformat

from fastavro import writer, parse_schema

from pfb_exporter.config import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PFB_FILE,
    SQLA_MODELS_FILE,
    DEFAULT_METADATA_FILE,
    DEFAULT_AVRO_SCHEMA_NAMESPACE
)
from pfb_exporter.utils import setup_logger, log_time_elapsed
from pfb_exporter.schema import PfbFileSchema
from pfb_exporter.entity import PfbMetadataEntity, PfbTableRowEntity
from pfb_exporter.sqla import SqlaModelBuilder
from pfb_exporter.db import DbUtils


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

        :param data_dir: A directory of JSON ND files. See
        _yield_entities_from_file for details
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
        self.output_dir = os.path.abspath(os.path.expanduser(output_dir))
        self.models_path = models_path or os.path.join(
            self.output_dir, SQLA_MODELS_FILE
        )
        self.namespace = namespace
        self.pfb_file = os.path.join(self.output_dir, DEFAULT_PFB_FILE)

    @log_time_elapsed
    def create_pfb_schema(self):
        """
        Build the PFB File's Avro schema from SQLAlchemy model classes

        If `db_conn_url` is provided, generate the model classes from the
        db and write them to `models_path`. Then import the SQLAlchemy model
        classes from the file at `models_path`
        """
        try:
            # Import the SQLAlchemy model classes from file
            self.model_builder = SqlaModelBuilder(
                self.models_path,
                self.output_dir,
                db_conn_url=self.db_conn_url
            )
            self.model_builder.create_and_import_models()

            # Check that model classes were imported
            if not self.model_builder.imported_model_classes:
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
        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info(
                f'❌ Create PFB schema {self.pfb_file_schema} failed!'
            )
            exit(1)
        else:
            self.logger.info(
                f'✅ Create PFB schema {self.pfb_file_schema} succeeded!'
            )

    @log_time_elapsed
    def build(
        self, table_name=None, sql_file=None, rm_pfb=True
    ):
        """
        Build PFB entities and write to an Avro file

        1. Create the PFB File Avro schema
        2. Stream in the data in from JSON ND files or the database.
        3. Write each PFB Entity as its yielded to the output Avro file

        Input Data: JSON ND
        -------------------
        If db_conn_url not provided then assume input data is in data_dir
        See _yield_row_entities_from_ndjson for details

        Input Data: Database
        --------------------
        If db_conn_url is provided then assume input data will be streamed
        from the database.
        See _yield_row_entities_from_db for details

        :param rm_file: whether or not to delete the PFB file if it exists
        :type rm_file: bool
        :param table_name: forwarded to _yield_row_entities_from_db
        :type table_name: str
        :param sql_file: forwarded to _yield_row_entities_from_db
        :type sql_file: str
        :param rm_pfb: A flag indicating whether existing PFB file should be
        removed
        :type rm_pfb: bool
        """
        try:
            # Import the SQLAlchemy model classes from file
            # Build the PFB file Avro schema
            self.create_pfb_schema()

            # Build the PFB file entities and write to the Avro file
            if self.db_conn_url:
                g = self._yield_row_entities_from_db(table_name, sql_file)
            else:
                g = self._yield_row_entities_from_ndjson()

            self.logger.info(
                f'✏️ Begin writing PFB Avro file to {self.pfb_file}'
            )

            # Remove previous pfb_file
            if rm_pfb and os.path.isfile(self.pfb_file):
                self.logger.info(
                    f'Deleting previous PFB File: {self.pfb_file}'
                )
                os.remove(self.pfb_file)

            parsed_schema = parse_schema(self.pfb_file_schema.avro_schema)
            with open(self.pfb_file, 'a+b') as Avro_file:
                for ent in g:
                    writer(Avro_file, parsed_schema, [ent], validator=True)

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info(f'❌ Export to PFB file {self.pfb_file} failed!')
            exit(1)
        else:
            self.logger.info(
                f'✅ Export to PFB file {self.pfb_file} succeeded!'
            )

    def yield_entities(self, table_row_entity_generator=None):
        """
        Return a generator over the PFB Entities needed to build a PFB File

        :param table_row_entity_generator: the generator used to produce
        Table Row PFB Entities
        :type table_row_entity_generator: generator
        """
        return chain(
            self._yield_metadata_entity(),
            table_row_entity_generator or
            self._yield_row_entities_from_ndjson()
        )

    def _yield_row_entities_from_db(self, table_name=None, sql_file=None):
        """
        Build Table Row PFB Entities from rows of data streamed from the
        database.

        The data to be streamed from the database can be selected via a
        SQL query in `sql_file`. If the `sql_file` option is used, the
        `table_name` option must also be provided, since it is used to
        specify which PFB Entities will be built from resulting rows. The
        table name is also used as the PFB Entity name.

        Additionally, resulting rows must conform to the table's
        schema, since the PFB Entity object properties reflect the table's
        columns.

        If a query is not provided, then all rows from the table will be
        selected.

        :param table_name: name of table in database, and type of PFB Entity
        to build from query results
        :type table_name: str
        :param sql_file: path to a file containing a SQL query
        :type sql_file: str
        """
        db_utils = DbUtils(
            self.db_conn_url, output_dir=self.output_dir, log=False
        )
        # Select subset of database and create PFB Entities conforming to table
        # table_name schema
        queries = None
        if table_name or sql_file:
            query = db_utils.load_query(
                table_name=table_name, sql_file_path=sql_file
            )
            queries = [(table_name, query)]
        # Select whole database and create a set of PFB entites for each table
        elif self.db_conn_url:
            queries = [
                (table_name, db_utils.load_query(table_name))
                for table_name in self.model_builder.orm_models_dict.keys()
            ]
        # Insufficient inputs to build PFB from DB
        if not queries:
            inputs = {
                'table_name': table_name,
                'sql_file': sql_file
            }
            raise RuntimeError(
                'Cannot build PFB from database. Not enough inputs:\n'
                f'{pformat(inputs)}. You must provide a valid SQL query to '
                'select data from the database AND the name of the table '
                'that the query results conform to. OR you can provide '
                'neither of those, and the all data from the database will be '
                'selected for PFB file creation.'
            )

        for table_name, query in queries:
            for i, r in enumerate(db_utils.yield_row_dicts(query, table_name)):
                yield PfbTableRowEntity(
                    i, r,
                    self.model_builder.orm_models_dict[table_name],
                    self.namespace
                ).data

    def _yield_row_entities_from_ndjson(self):
        """
        Create and yield a Table Row PFB Entity dict for each record in a
        properly formatted JSON ND file within the data_dir directory.

        See pfb_exporter.entity.PfbTableRowEntity for details on dict

        The ND JSON file is expected to contain row data from a single table
        in a database. Each JSON record after the first record represents a
        PFB Entity.

        The file MUST follow this format:

            "biospecimen"
            { biospecimen JSON object 0 }
                        ...
            { biospecimen JSON object N }

        - The first row must consist of a single JSON string. It must specify
          the table name from which subsequent JSON object records in the file
          came from. The table name is used as the PFB Entity type.

        - The rest of the records in the file after the first one must be
          valid JSON objects which conform to the table schema.
        """
        self.logger.info(
            f'Begin creating PFB Entities from files in {self.data_dir}'
        )

        def process_first(obj):
            msg = (
                f'First line of the ndjson file must a string '
                'specifying the name of the table from which all subsequent '
                'JSON records in the file conform to (e.g. "biospecimen")'
            )
            if not isinstance(obj, str):
                raise TypeError(msg)

            tables = set(self.model_builder.orm_models_dict.keys())
            if obj not in tables:
                raise ValueError(
                    msg + 'Table name must be one of:\n'
                    f'{tables}'
                )
            return obj

        # Process JSON ND files in data dir
        for fi, fn in enumerate(os.listdir(self.data_dir)):
            pth = os.path.join(self.data_dir, fn)
            if os.path.isdir(pth):
                continue

            self.logger.info(f'Processing file #{fi}: {pth}')

            table_name = None
            with jsonlines.open(pth) as ndjson_reader:
                for i, obj in enumerate(ndjson_reader):
                    # Make sure first record has table name
                    if i == 0:
                        table_name = process_first(obj)
                        self.logger.info(f'Detected table name: {table_name}')
                    else:
                        # Transform the JSON object into a Table Row PFB Entity
                        yield PfbTableRowEntity(
                            i,
                            obj,
                            self.model_builder.orm_models_dict[table_name],
                            self.namespace
                        ).data

    def _yield_metadata_entity(self):
        """
        Create and yield Metadata PFB Entity dict.

        Write Metadata dict to a JSON file:
        pfb_exporter.config.DEFAULT_METADATA_FILE in the output directory

        See pfb_exporter.entity.PfbMetadataEntity for details on dict
        """
        metadata = PfbMetadataEntity(
            self.model_builder.orm_models_dict, self.namespace
        ).data

        self.metadata_file = os.path.join(
            self.output_dir, DEFAULT_METADATA_FILE
        )

        self.logger.info(
            f'✏️ Writing Metadata PFB Entity to {self.metadata_file}'
        )

        with open(self.metadata_file, 'w') as json_file:
            json.dump(metadata, json_file, indent=4, sort_keys=True)

        yield metadata
