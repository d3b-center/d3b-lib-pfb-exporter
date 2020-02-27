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
from collections import defaultdict
from pprint import pformat, pprint

from sqlalchemy.inspection import inspect as sqla_inspect
from sqlalchemy.orm.properties import ColumnProperty

from pfb_exporter.config import DEFAULT_PFB_SCHEMA_FILE

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
    def __init__(self, model_dict, output_dir):
        self.logger = logging.getLogger(type(self).__name__)
        self.model_dict = model_dict
        self.output_dir = output_dir

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
        model_schema_template = defaultdict(list)
        pfb_schema = {}

        for model_name, model_cls in self.model_dict.items():
            self.logger.info(
                f'Building schema for {model_name} ...'
            )
            model_schema = deepcopy(model_schema_template)
            # Inspect model columns and types
            for p in sqla_inspect(model_cls).iterate_properties:
                if not isinstance(p, ColumnProperty):
                    continue

                if not hasattr(p, 'columns'):
                    continue

                schema_dict = self._column_obj_to_schema_dict(
                    p.key, p.columns[0]
                )
                if schema_dict['type'] == 'foreign_key':
                    model_schema['foreign_keys'] = schema_dict
                else:
                    model_schema['attributes'].append(schema_dict)

            self.logger.debug(f'{pformat(model_schema)}')
            pfb_schema[model_cls.__tablename__] = model_schema

        self._write_pfb_schema(pfb_schema)

        return pfb_schema

    def _column_obj_to_schema_dict(self, key, column_obj):
        """
        Convert a SQLAlchemy Column object to a schema dict
        """
        # Check if foreign key
        if column_obj.foreign_keys:
            fkname = column_obj.foreign_keys.pop().target_fullname
            return {
                'relation': fkname.split('.')[0],
                'name': key,
                'type': 'foreign_key'
            }

        # Convert SQLAlchemy column type to avro type
        stype = type(column_obj.type).__name__

        # Get avro primitive type
        ptype = SQLA_AVRO_TYPE_MAP['primitive'].get(stype)
        if not ptype:
            self.logger.warning(
                f'⚠️ Could not find avro type for {key}, '
                f'SQLAlchemy type: {stype}'
            )
        attr_dict = {'name': key, 'type': ptype}

        # Get avro logical type if applicable
        ltype = SQLA_AVRO_TYPE_MAP['logical'].get(stype)
        if ltype:
            attr_dict.update({'logicalType': ltype})

        # Get default value for attr
        # if column_obj.default:
        #     attr_dict.update({'default': column_obj.default})

        if column_obj.nullable:
            attr_dict['type'] = ['null', attr_dict['type']]

        return attr_dict

    def _write_pfb_schema(self, data):
        """
        Write the Gen3 data dictionary created by self.transform to the
        output_dir as a set of yaml files. There will be one YAML file per
        entity

        :param data: data needed to write out the Gen3 data dict files
        :type data: dict
        :returns: path to directory containing data dict files
        """
        self.pfb_schema = os.path.join(
            self.output_dir, DEFAULT_PFB_SCHEMA_FILE
        )
        if data:
            self.logger.info(
                f'✏️ Writing PFB schema to {self.pfb_schema}'
            )
            with open(self.pfb_schema, 'w') as json_file:
                json.dump(data, json_file, indent=4, sort_keys=True)
