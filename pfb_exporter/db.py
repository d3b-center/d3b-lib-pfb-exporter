import os
import logging
from datetime import datetime

import jsonlines
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import text

from pfb_exporter.config import DEFAULT_OUTPUT_DIR
from pfb_exporter.utils import (
    setup_logger,
    iso_8601_datetime_str,
    log_time_elapsed
)


class DbUtils(object):
    def __init__(
        self,
        db_conn_url,
        output_dir=DEFAULT_OUTPUT_DIR,
        log=True
    ):
        """
        Constructor

        :param db_conn_url: The database connection URL.
        Example: postgresql://postgres:mypassword@127.0.0.1:5432/mydb
        :type db_conn_url: str
        :param output_dir: directory where all all outputs are written
        :type output_dir: str
        :param log: whether or not to setup a console and file logger
        :type log: bool
        """
        if log:
            setup_logger(os.path.join(output_dir, 'logs'), filename='db.log')

        self.logger = logging.getLogger(type(self).__name__)
        self.db_conn_url = db_conn_url
        self.output_dir = output_dir

        self.engine = create_engine(db_conn_url)
        self.session = self.engine.connect()
        self.logger.info('☑️ Connected to database')

    @log_time_elapsed
    def db_to_ndjson(self):
        """
        Export each table in database to a JSON ND file

        See table_rows_to_ndjson for details
        """
        m = MetaData()
        m.reflect(self.engine)
        for table_name in m.tables.keys():
            self.table_rows_to_ndjson(table_name)

    @log_time_elapsed
    def table_rows_to_ndjson(self, table_name, sql_file_path=None):
        """
        Stream rows of data from the database to a JSON ND file.

        The rows to stream will be selected using the SQL query in the file
        `sql_file_path` or, if the query is not provided, all rows from table
        `table_name` will be selected.

        Since this is producing a JSON ND file specifically formatted for
        the pfb_exporter.builder.PfbFileBuilder, each row resulting from the
        SQL query must conform to the table's `table_name` schema.

        The file will be written to <output_dir>/<table_name>.ndjson
        and follow this format:

            "biospecimen"
            { biospecimen JSON object 0 }
                        ...
            { biospecimen JSON object N }

        The first JSON object in the file will be the name of the table that
        subsequent JSON objects conform to. Any JSON object after the first one
        in the file captures the row data conforming to that table's schema.

        :param table_name: name
        """
        self.logger.info(f'Creating JSON ND file for {table_name} rows')

        os.makedirs(self.output_dir, exist_ok=True)
        output_ndjson_file = os.path.join(
            self.output_dir, table_name + '.ndjson'
        )

        query = self.load_query(
            table_name=table_name, sql_file_path=sql_file_path
        )

        with jsonlines.open(output_ndjson_file, 'w') as writer:
            writer.write(table_name)
            for row_dict in self.yield_row_dicts(query, table_name):
                writer.write(row_dict)

        self.logger.info(f'✏️ Wrote file to {output_ndjson_file}')

    def load_query(self, table_name=None, sql_file_path=None):
        """
        Load a SQL query from file or create the default query:
        select * from <table_name>.

        Raise a RuntimeError exception if a query cannot be loaded
        """
        query = None

        if sql_file_path:
            sql_file_path = os.path.abspath(os.path.expanduser(sql_file_path))
            self.logger.info(f'Loading SQL query from {sql_file_path}')
            with open(sql_file_path, 'r') as sql_file:
                query = sql_file.read().strip()

        elif table_name:
            self.logger.info('Loading default select all query')
            query = f"select * from {table_name};"

        if not query:
            raise RuntimeError(
                'Cannot load a selection query without either the '
                'table name or a provided SQL query!'
            )
        else:
            self.logger.info(f'Loaded SQL query:\n{query}')

        return query

    def yield_row_dicts(self, query, entity_type=''):
        """
        Execute a SQL query and return a generator over the resulting
        row dicts

        :param query: SQL query to execute
        :type query: str
        :param entity_type: The type of entity (table) each resulting row
        represents
        """
        basic_types = {int, float, bool, str, bytes}

        for i, result_proxy in enumerate(self.session.execute(text(query))):
            self.logger.info(f'Fetching {entity_type} record #{i}')
            row_dict = {}
            for col, val in result_proxy.items():
                if not ((val is None) or (type(val) in basic_types)):
                    if isinstance(val, datetime):
                        val = iso_8601_datetime_str(val)
                    else:
                        val = str(val)

                row_dict[col] = val

            yield row_dict
