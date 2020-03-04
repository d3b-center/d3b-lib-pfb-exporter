"""
Import the SQLAlchemy declarative base classes from a file or directory
Optionally generate the classes by inspecting the DB schema using the
sqlacodegen (https://github.com/agronholm/sqlacodegen) library.

NOTE
----
A declarative base is a class representation of a table in a relational
database.

The term "SQLAlchemy model" class will be synonymous with "SQLAlchemy
declarative base" class throughout the docstrings in this module.
"""
import os
import json
import inspect
import logging
import subprocess
from copy import deepcopy
import timeit
from pprint import pformat, pprint

from sqlalchemy.inspection import inspect as sqla_inspect
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.exc import NoInspectionAvailable


from pfb_exporter.utils import import_module_from_file, seconds_to_hms

from pfb_exporter.config import ORM_MODELS_FILE


class SqlaModelBuilder(object):

    def __init__(
            self, models_path, output_dir, db_conn_url=None
    ):
        """
        Constructor

        :param models_path: path to where the SQLAlchemy model classes are
        stored or will be written if they are generated
        :type models_path: str
        :param output_dir: directory where serialized model classes will be
        written
        :type output_dir: str
        :param db_conn_url: Connection URL for database. Format depends on
        database. See SQLAlchemy documentation for supported databases
        :type db_conn_url: str
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.models_path = models_path
        self.output_dir = output_dir
        self.db_conn_url = db_conn_url
        self.imported_model_classes = []
        self.orm_models_dict = {}

    def create_and_import_models(self):
        """
        Import the SQLAlchemy model classes from a file or directory

        Optionally generate the classes by inspecting the DB schema using the
        sqlacodegen library

        Serialize the model classes to a dict and write the dict to file
        """
        # Generate the SQLAlchemy model classes from the database and write
        # to a Python module
        if self.db_conn_url:
            self._generate_models()

        # Import the SQLAlchemy model classes
        self.imported_model_classes = self._import_models()

        # Serialize the SQLAlchemy model classes to dicts
        self.orm_models_dict = self.to_dict()

        # Write the serialized model classes to a JSON file
        self._write_output()

    def _generate_models(self):
        """
        Generate SQLAlchemy models from a database using sqlacodegen CLI

        See https://github.com/agronholm/sqlacodegen
        """
        # sqlacodegen requires the models to be written to a file
        if os.path.isdir(self.models_path):
            self.models_path = os.path.join(
                self.models_path, 'models.py'
            )

        # Generate SQLAlchemy models
        cmd_str = (
            f'sqlacodegen {self.db_conn_url} --outfile {self.models_path}'
        )
        self.logger.debug(f'Building SQLAlchemy models:\n{cmd_str}')

        start_time = timeit.default_timer()
        output = subprocess.run(
            cmd_str, shell=True, stdout=subprocess.PIPE
        )
        total_time = timeit.default_timer() - start_time

        output.check_returncode()

        self.logger.debug(f'Time elapsed: {seconds_to_hms(total_time)}')

    def _import_models(self):
        """
        Import the SQLAlchemy model classes from the Python modules
        in self.models_path
        """
        imported_model_classes = []

        self.logger.debug(
            f'Importing SQLAlchemy models from {self.models_path}'
        )

        def _import_model_classes_from_file(filepath):
            """
            Import the SQLAlchemy models from the Python module at `filepath`
            """
            imported_model_classes = []
            mod = import_module_from_file(filepath)
            # NOTE - We cannot use issubclass to test if the SQLAlchemy model
            # class is a subclass of its parent (sqlalchemy.ext.declarative.api.Base) # noqa E5501
            # The best we can do is make sure the class is a SQLAlchemy object
            # and check that the object is a DeclarativeMeta type
            for cls_name, cls_path in inspect.getmembers(mod, inspect.isclass):
                cls = getattr(mod, cls_name)
                try:
                    sqla_inspect(cls)
                except NoInspectionAvailable:
                    # Not a SQLAlchemy object
                    pass
                else:
                    if type(cls) == DeclarativeMeta:
                        imported_model_classes.append(cls)

            return imported_model_classes

        if (os.path.isfile(self.models_path) and
                os.path.splitext(self.models_path)[-1] == '.py'):
            filepaths = [self.models_path]
        else:
            filepaths = [
                os.path.join(root, fn)
                for root, dirs, files in os.walk(self.models_path)
                for fn in files
                if os.path.splitext(fn)[-1] == '.py'
            ]

        self.logger.debug(
            f'Found {len(filepaths)} Python modules:\n{pformat(filepaths)}'
        )
        # Import the model classes from file(s)
        for fp in filepaths:
            imported_model_classes.extend(_import_model_classes_from_file(fp))

        self.logger.info(
            f'Imported {len(imported_model_classes)} SQLAlchemy models:'
            f'\n{pformat([m.__name__ for m in imported_model_classes])}'
        )

        return imported_model_classes

    def to_dict(self):
        """
        Serialize each SQLAlchemy model into a dict that captures
        a each model's name, attributes, types of attributes, and foreign keys.

        The dict will look like:

        {
            '<table name>': {
                'class': <full module path to the model class>,
                'table_name': <table name>,
                'properties': [
                    {
                        Output of _column_obj_to_dict
                    },
                    ...
                ],
                'foreign_keys': [
                    {
                        'attribute': <foreign key column name>,
                        'table': <foreign key table name>
                    }
                ]
            }
        }
        """
        orm_models_dict = {}

        self.logger.info(
            'Serializing SQLAlchemy models to dicts'
        )
        model_dict_template = {
            'class': None,
            'table_name': None,
            'properties': [],
            'foreign_keys': []
        }

        for model_cls in self.imported_model_classes:
            model_dict = deepcopy(model_dict_template)
            model_name = model_cls.__name__
            model_dict['table_name'] = model_cls.__tablename__
            model_dict['class'] = model_name

            self.logger.info(
                f'Building model dict for {model_name} ...'
            )

            # Inspect model columns and types
            for p in sqla_inspect(model_cls).iterate_properties:
                if not isinstance(p, ColumnProperty):
                    continue

                if not hasattr(p, 'columns'):
                    continue

                d = self._column_obj_to_dict(p.key, p.columns[0])

                if d.pop('primary_key', None):
                    model_dict['primary_key'] = p.key

                model_dict['properties'].append(d)

                fk = d.pop('foreign_key', {})
                if fk:
                    model_dict['foreign_keys'].append(fk)

            orm_models_dict[model_dict['table_name']] = model_dict

        return orm_models_dict

    def _column_obj_to_dict(self, column_name, column_obj):
        """
        Convert a sqlalchemy.sql.schema.Column.type object to a dict

        :param column_name: name of the table column
        :type column_name: str
        :param column_obj: the SQLAlchemy object representing a column schema
        :type column_obj: sqlalchemy.sql.schema.Column.type
        """
        # Get SQLAlchemy column type
        sqla_type = type(column_obj.type).__name__
        column_dict = {
            'class': str(type(column_obj.type)),
            'sqla_type': sqla_type,
            'name': column_name,
            'nullable': column_obj.nullable,
            'default': column_obj.default,
            'doc': column_obj.doc or '',
            'primary_key': column_obj.primary_key
        }
        # Check if array
        if sqla_type == 'ARRAY':
            column_dict['item_type'] = type(column_obj.type.item_type).__name__

        # Check if foreign key
        if column_obj.foreign_keys:
            fkname = column_obj.foreign_keys.pop().target_fullname
            table_name, fk_col_name = tuple(fkname.split('.'))
            column_dict.update({
                'foreign_key': {
                    'fkname': column_name,
                    'column': fk_col_name,
                    'table': table_name
                }
            })

        return column_dict

    def _write_output(self):
        """
        Write the output of SqlaModelBuilder.to_dict to a JSON file
        """
        self.orm_models_file = os.path.join(
            self.output_dir, ORM_MODELS_FILE
        )
        if self.orm_models_dict:
            self.logger.info(
                '✏️ Writing ORM model dict to '
                f'{self.orm_models_file}'
            )
            with open(self.orm_models_file, 'w') as json_file:
                json.dump(
                    self.orm_models_dict, json_file, indent=4, sort_keys=True
                )
