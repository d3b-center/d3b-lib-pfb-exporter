"""
A module that stores classes to represent different Avro record schemas in a
PFB file and the Avro schema for the PFB file itself.

A PFB file contains two types of PFB Entities. One (Metadata) captures
information about the relational database and the other (Table) captures a row
of data from a particular table in the database.

See class docstrings for more information on the Avro schemas for each type
of PFB Entity and the PFB file.

For more info on PFB see:
- See https://github.com/uc-cdis/pypfb#pfb-schema

For more info on Avro schema see:
- See https://avro.apache.org/docs/current/spec.html#schemas
"""

import os
import json
import logging
from copy import deepcopy

from pfb_exporter.config import (
    DEFAULT_PFB_SCHEMA_FILE,
    PFB_SCHEMA_TEMPLATE,
    METADATA_SCHEMA_TEMPLATE
)

SQLA_AVRO_TYPE_MAP = {
    'other': {
        'Text': 'string',
        'Boolean': 'boolean',
        'Float': 'float',
        'Integer': 'int',
        'String': 'string',
        'UUID': 'string',
        'DateTime': 'string',
        'ARRAY': 'array'
    },
    'logical': {
        'UUID': 'uuid',
        'DateTime': None
    }
}


class PfbFileSchema(object):
    """
    A class that represents the Avro schema for a PFB File. A PFB file is
    special kind of Avro file, suitable for capturing and reconstructing
    biomedical relational data.

    A PFB File contains of a list of JSON objects:

        0   PFB Metadata Entity     --> First entity will always be Metadata
        1   PFB Table Row Entity    --> Remaining entity will be Table Row
        ...                             entities from different DB tables

    A PFB file's avro schema looks like this (does have not correct syntax):

    {
        name: Entity,
        type: record,
        fields: [
            { name: id, type: string },
            { name: name, type: string },
            {
                name: object,
                type: any of (
                    PFB Metadata Entity schema,
                    PFB Table Row Entity schema
                )
            }
        ]
    }

    See templates/pfb_schema.json to view the PFB file's actual avro schema

    See PfbMetadataEntitySchema and PfbTableRowEntitySchema for details on
    other avro record schemas
    """

    def __init__(self, orm_models_dict, output_dir, namespace):
        """
        Build the avro schema for the PFB file and write to a JSON file

        :param orm_models_dict: the output of
        pfb_exporter.sqla.SqlaModelBuilder.to_dict. Captures the components
        of a database table schemas: col names, types, foreign keys, etc
        :type orm_models_dict: dict
        :param output_dir: directory where PFB file avro schema is written
        :type output_dir: str
        :param namespace: the Avro schema namespace
        :type namespace: str
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.orm_models_dict = orm_models_dict
        self.output_dir = output_dir
        self.namespace = namespace
        self.avro_template_file = PFB_SCHEMA_TEMPLATE
        self.metadata_schema = PfbMetadataEntitySchema()
        self.table_row_schemas = self._build_table_row_schemas()
        self.avro_schema = self.build()
        self._write_output()

    def build(self):
        """
        Create the PFB file avro schema dict from the Metadata PFB Entity
        schema and the table row PFB entity schemas.
        """
        self.logger.info('Building avro schema for PFB File')
        with open(self.avro_template_file, 'r') as json_file:
            pfb_schema = json.load(json_file)

        pfb_schema['namespace'] = self.namespace

        # Add metadata and table row entity avro schemas to object schemas
        object_schemas = [self.metadata_schema.avro_schema] + [
            schema_obj.avro_schema
            for schema_obj in self.table_row_schemas
        ]

        # Insert into PFB file schema
        try:
            for f in pfb_schema['fields']:
                if f['name'] == 'object':
                    f['type'] = object_schemas
                    break

            assert object_schemas, (
                'Could not find the `object` field in '
                'pfb_schema_template.fields'
            )

        except Exception as e:
            self.logger.warning(
                'Error parsing PFB file schema template '
                f'{pfb_schema}! '
                'Check to make sure it is not malformed'
            )
            raise e

        return pfb_schema

    def _build_table_row_schemas(self):
        """
        Build PfbTableRowEntitySchema objects from the orm_models_dict
        """
        return [
            PfbTableRowEntitySchema(model_dict)
            for model_dict in self.orm_models_dict.values()
        ]

    def _write_output(self):
        """
        Write the avro schema to a JSON file
        """
        self.avro_schema_file = os.path.join(
            self.output_dir, DEFAULT_PFB_SCHEMA_FILE
        )
        if self.avro_schema:
            self.logger.info(
                '✏️ Writing avro schema for PFB File to '
                f'{self.avro_schema_file}'
            )
            with open(self.avro_schema_file, 'w') as json_file:
                json.dump(
                    self.avro_schema, json_file, indent=4, sort_keys=True
                )


class PfbMetadataEntitySchema(object):
    """
    A class that represents the Avro schema for the Metadata PFB Entity which
    captures metadata about the tables in a biomedical relational database.

    The Metadata PFB Entity captures:
        - Table relationships
        - Table attributes
        - Which ontology a table is tied to if any
        - Which ontology concept a table attribute is tied to if any

    See METADATA_SCHEMA_TEMPLATE for structure of the Metadata Avro schema
    """

    def __init__(self):
        """
        Build the avro schema for the Metadata PFB Entity
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.avro_template_filepath = METADATA_SCHEMA_TEMPLATE
        self.avro_schema = self.build()

    def build(self):
        self.logger.info('Building avro schema for Metadata PFB Entity')
        with open(self.avro_template_filepath, 'r') as json_file:
            metadata_schema = json.load(json_file)
        return metadata_schema


class PfbTableRowEntitySchema(object):
    """
    A class that represents the Avro schema for a PFB Entity which
    captures a single row of data from a particular table in the database

    The structure from one PFB table Entity to another varies because the
    Avro schema is dynamically generated by inspecting the table
    schema. Table schema inspection is done by generating a SQLAlchemy model
    for the table schema.

    See PfbTableRowEntitySchema.build for details.
    """

    def __init__(self, orm_model_dict):
        """
        Build the avro schema for the Metadata PFB Entity

        :param orm_model_dict: a value from the dict output of
        pfb_exporter.sqla.SqlaModelBuilder.to_dict. Captures the components of
        a database table's schema: col names, types, foreign keys, etc
        :type orm_model_dict: dict
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.orm_model_dict = orm_model_dict
        self.avro_schema = self.build()

    def build(self):
        """
        Build the avro schema for a PFB Entity representing a single row
        of data from a table in the database.

        :returns: the avro schema for a table row within the database
        """
        self.logger.info(
            'Building avro schema for Table Row PFB Entity: '
            f'{self.orm_model_dict["table_name"]}'
        )
        avro_schema = {
            'type': 'record',
            'name': self.orm_model_dict['table_name'],
            'fields': []
        }
        for p in self.orm_model_dict['properties']:
            self.logger.debug(
                f'Building avro schema for field: {p["name"]}'
            )
            prop_schema = deepcopy({})
            prop_schema.update(self._create_avro_types(p))
            prop_schema['default'] = p['default']
            prop_schema['name'] = p['name']
            prop_schema['doc'] = p['doc']
            avro_schema['fields'].append(prop_schema)

        return avro_schema

    def _create_avro_types(self, column_obj_dict):
        """
        Create Avro types (primitive/complex types and logical type) from the
        orm_model_dict's column dict.

        :param column_obj_dict: See pfb_exporter.sqla._column_obj_to_dict
        :type column_obj_dict: dict
        :returns: an avro types dict
        """
        output = {}

        def sqla_to_avro_type(sqla_type, is_logical=False):
            if is_logical:
                # Check if logical type exists
                avro_type = SQLA_AVRO_TYPE_MAP['logical'].get(sqla_type)
            else:
                avro_type = SQLA_AVRO_TYPE_MAP['other'].get(sqla_type)
                if not avro_type:
                    self.logger.error(
                        f'⚠️ Could not find avro type for {key}, '
                        f'SQLAlchemy type: {sqla_type}'
                    )

            return avro_type

        stype = column_obj_dict['sqla_type']
        key = column_obj_dict['name']

        # Get avro type
        avro_type = sqla_to_avro_type(stype)
        output['type'] = avro_type

        # Array type
        if avro_type == 'array':
            output['type'] = {
                'type': avro_type,
                'items': sqla_to_avro_type(column_obj_dict.get('item_type'))
            }

        # Add null to list of allowed types if nullable
        if column_obj_dict['nullable']:
            output['type'] = ['null', deepcopy(output['type'])]

        # Get avro logical type if applicable
        avro_ltype = sqla_to_avro_type(stype, is_logical=True)
        if avro_ltype:
            output['logicalType'] = avro_ltype

        return output
