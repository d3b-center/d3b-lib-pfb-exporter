"""
Contains helper class to transform SQLAlchemy model into PFB schema.

A PFB Schema is just and Avro schema (a JSON blob) which describes the
PFB graph.

For more info on PFB see:
- See https://github.com/uc-cdis/pypfb#pfb-schema
- See d3b-lib-pfb-exporter/template/pfb_schema.json to view PFB schema
template

For more info on Avro schema see:
- See https://avro.apache.org/docs/current/spec.html#schemas
"""

import os
import json
import logging
from copy import deepcopy
from pprint import pformat, pprint

from sqlalchemy.inspection import inspect as sqla_inspect
from sqlalchemy.orm.properties import ColumnProperty

from pfb_exporter.config import DEFAULT_PFB_SCHEMA_FILE, REL_MODEL_FILE

SQLA_AVRO_TYPE_MAP = {
    'primitive': {
        'Text': 'string',
        'Boolean': 'boolean',
        'Float': 'float',
        'Integer': 'int',
        'String': 'string',
        'UUID': 'string',
        'DateTime': 'string',
    },
    'logical': {
        'UUID': 'uuid',
        'DateTime': None
    }
}


class PfbSchemaBuilder(object):
    def __init__(self, imported_models, output_dir):
        self.logger = logging.getLogger(type(self).__name__)
        self.imported_models = imported_models
        self.output_dir = output_dir
        self.relational_model = []
        self.pfb_schema = {}

    def create(self):
        """
        Transform SQLAlchemy model into PFB schema. A PFB Schema is just
        and Avro schema (a JSON blob) which describes the PFB graph.

        For more info on PFB see:
        - See https://github.com/uc-cdis/pypfb#pfb-schema
        - See d3b-lib-pfb-exporter/template/pfb_schema.json to view PFB schema
        template

        For more info on Avro schema see:
        - See https://avro.apache.org/docs/current/spec.html#schemas
        """
        self.logger.info('Creating PFB schema from SQLAlchemy models ...')

        self._build_relational_model()
        self._relational_model_to_pfb_schema()
        self._write_outputs()

        return self.pfb_schema

    def _build_relational_model(self):
        """
        Serialize the SQLAlchemy models into a dictionary that captures
        a model's name, attributes, types of attributes, and foreign keys

        This is an intermediate form which serves 2 purposes:

        - Make it easier to transform into the PFB schema
        - Used by pfb_exporter.transform.entity_builder to produce the
          PFB Metadata object
        """
        self.logger.info(
            'Serializing SQLAlchemy models into the intermediate form'
        )
        model_dict_template = {
            'class': None,
            'table_name': None,
            'properties': {},
            'foreign_keys': {}
        }

        for model_cls in self.imported_models:
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

                d = self._column_obj_to_dict(
                    p.key, p.columns[0]
                )
                model_dict['foreign_keys'].update(d.pop('foreign_key', {}))
                model_dict['properties'].update(d)

            self.logger.debug(f'\n{pformat(model_dict)}')

            self.relational_model.append(model_dict)

    def _relational_model_to_pfb_schema(self):
        pass

    def _column_obj_to_dict(self, key, column_obj):
        """
        Convert a SQLAlchemy Column object to a dict
        """
        column_dict = {}

        # Get SQLAlchemy column type
        ctype = type(column_obj.type).__name__
        column_dict[key] = {
            'type': ctype,
            'nullable': column_obj.nullable,
            'default': column_obj.default
        }
        # Check if foreign key
        if column_obj.foreign_keys:
            fkname = column_obj.foreign_keys.pop().target_fullname
            column_dict.update({
                'foreign_key': {fkname: fkname.split('.')[0]}
            })

        return column_dict

    def _write_outputs(self):
        """
        Write the PFB Schema dict to a JSON file
        Write the relational model dict to a JSON file
        """
        self.pfb_schema_file = os.path.join(
            self.output_dir, DEFAULT_PFB_SCHEMA_FILE
        )
        self.rel_model_file = os.path.join(
            self.output_dir, REL_MODEL_FILE
        )
        if self.pfb_schema:
            self.logger.info(
                f'✏️ Writing PFB schema to {self.pfb_schema_file}'
            )
            with open(self.pfb_schema_file, 'w') as json_file:
                json.dump(self.pfb_schema, json_file, indent=4, sort_keys=True)

        if self.relational_model:
            self.logger.info(
                '✏️ Writing relational model dict to '
                f'{self.rel_model_file}'
            )
            with open(self.rel_model_file, 'w') as json_file:
                json.dump(
                    self.relational_model, json_file, indent=4, sort_keys=True
                )
