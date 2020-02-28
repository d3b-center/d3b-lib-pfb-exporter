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

from pfb_exporter.config import (
    DEFAULT_PFB_SCHEMA_FILE,
    PFB_SCHEMA_TEMPLATE,
    REL_MODEL_FILE
)

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

        1. Transform SQLAlchemy model into dicts first
            - See PfbBuilder._build_relational_model

        2. Transform from the dicts to PFB Schema dict
            - See PfbBuilder._relational_model_to_pfb_schema

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
        Serialize each SQLAlchemy model into a dict that captures
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
            'properties': [],
            'foreign_keys': []
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
                model_dict['properties'].append(d)

                fk = d.pop('foreign_key', {})
                if fk:
                    model_dict['foreign_keys'].append(fk)

            self.relational_model.append(model_dict)

    def _relational_model_to_pfb_schema(self):
        """
        Transform each model dict from PfbBuilder._build_relational_model
        into an Avro schema for a PFB Entity
        """
        with open(PFB_SCHEMA_TEMPLATE, 'r') as json_file:
            pfb_schema_template = json.load(json_file)

        pfb_schema = deepcopy(pfb_schema_template)

        # Get the part of the PFB schema we need to populate
        object_schemas = None
        try:
            for f in pfb_schema['fields']:
                if f['name'] == 'object':
                    object_schemas = f['type']
                    break

            assert object_schemas, (
                'Could not find the `object` field in '
                'pfb_schema_template.fields'
            )

        except Exception as e:
            self.logger.warning(
                'Error parsing PFB schema template '
                f'{self.pfb_schema_template}! '
                'Check to make sure it is not malformed'
            )
            raise e

        # Populate the PFB schema with our model schemas
        for model_dict in self.relational_model:
            props = model_dict['properties']
            model_schema = deepcopy({})
            model_schema = {
                'type': 'record',
                'name': model_dict['table_name'],
                'fields': []
            }

            for p in props:
                prop_schema = deepcopy({})
                prop_schema.update(self._get_avro_types(p))
                prop_schema['default'] = p['default']
                prop_schema['name'] = p['name']
                prop_schema['doc'] = p['doc']
                model_schema['fields'].append(prop_schema)

            object_schemas.append(model_schema)

        self.pfb_schema = pfb_schema

    def _get_avro_types(self, model_dict):
        """
        Get Avro types (primitive type and logical type) from the SQLAlchemy
        Column.type
        """
        output = {}
        stype = model_dict['type']
        key = model_dict['name']

        # Get avro primitive type
        ptype = SQLA_AVRO_TYPE_MAP['primitive'].get(stype)
        if not ptype:
            self.logger.warning(
                f'⚠️ Could not find avro type for {key}, '
                f'SQLAlchemy type: {stype}'
            )

        # Add null to list of allowed types if nullable
        if model_dict['nullable']:
            output['type'] = ['null', ptype]

        # Get avro logical type if applicable
        ltype = SQLA_AVRO_TYPE_MAP['logical'].get(stype)
        if ltype:
            output['logicalType'] = ltype

        return output

    def _column_obj_to_dict(self, key, column_obj):
        """
        Convert a SQLAlchemy Column object to a dict
        """
        # Get SQLAlchemy column type
        ctype = type(column_obj.type).__name__
        column_dict = {
            'type': ctype,
            'name': key,
            'nullable': column_obj.nullable,
            'default': column_obj.default,
            'doc': column_obj.doc or ''
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
