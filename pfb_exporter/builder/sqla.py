"""
Import SQLAlchemy model classes from file

Optional - Inspect DB and generate the SQLAlchemy model file
"""
import os
import logging
import inspect
import subprocess
import timeit
from pprint import pformat, pprint

from sqlalchemy.inspection import inspect as sqla_inspect
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.exc import NoInspectionAvailable


from pfb_exporter.utils import import_module_from_file, seconds_to_hms


class SqlaModelBuilder(object):

    def __init__(
            self, models_filepath, output_dir, db_conn_url=None
    ):
        """
        Constructor

        :param models_filepath: path to where the SQLAlchemy models are stored
        or will be written if they are generated
        :type models_filepath: str
        :param output_dir: path where PFB Schema will be written
        :type output_dir: str
        :param db_conn_url: Connection URL for database. Format depends on
        database. See SQLAlchemy documentation for supported databases
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.models_filepath = models_filepath
        self.output_dir = output_dir
        self.db_conn_url = db_conn_url
        self.imported_models = []

    def create_and_import_models(self):
        """
        Import SQLAlchemy model classes from file

        - (Optional) Inspect DB and generate the SQLAlchemy model file
        """

        if self.db_conn_url:
            self._generate_models()

        self._import_models()

        if not (self.db_conn_url or self.imported_models):
            raise RuntimeError(
                'There are 0 models to generate the PFB file. You must '
                'provide a DB connection URL that can be used to '
                'connect to a database to generate the models or '
                'provide a dir or file path to where the models reside'
            )

    def _generate_models(self):
        """
        Generate SQLAlchemy models from database

        Uses sqlacodegen CLI to generate models
        See https://github.com/agronholm/sqlacodegen
        """
        # sqlacodegen requires the models to be written to a file
        if os.path.isdir(self.models_filepath):
            self.models_filepath = os.path.join(
                self.models_filepath, 'models.py'
            )

        # Generate SQLAlchemy models
        cmd_str = (
            f'sqlacodegen {self.db_conn_url} --outfile {self.models_filepath}'
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
        in self.models_filepath
        """
        self.logger.debug(
            f'Importing SQLAlchemy models from {self.models_filepath}'
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

        if (os.path.isfile(self.models_filepath) and
                os.path.splitext(self.models_filepath)[-1] == '.py'):
            filepaths = [self.models_filepath]
        else:
            filepaths = [
                os.path.join(root, fn)
                for root, dirs, files in os.walk(self.models_filepath)
                for fn in files
                if os.path.splitext(fn)[-1] == '.py'
            ]

        self.logger.debug(
            f'Found {len(filepaths)} Python modules:\n{pformat(filepaths)}'
        )
        # Import the model classes from file
        for fp in filepaths:
            self.imported_models.extend(_import_model_classes_from_file(fp))

        self.logger.info(
            f'Imported {len(self.imported_models)} SQLAlchemy models:'
            f'\n{pformat([m.__name__ for m in self.imported_models])}'
        )
