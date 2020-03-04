"""
A module which stores classes representing the 2 different PFB Entities:
- Metadata Entity
- Table Row Entity

See pfb_exporter.schema for details on each type of PFB entity
"""
import os
import json
import logging
from copy import deepcopy
from pprint import pformat

from pfb_exporter.config import (
    METADATA_TEMPLATE,
    ENTITY_TEMPLATE
)


class PfbMetadataEntity(object):
    def __init__(self, orm_models_dict, namespace):
        """
        Builds the Metadata PFB Entity, a dict which captures a biomedical
        database's relational model (tables, relationships) and ontology
        references.

        :param orm_models_dict: the output of
        pfb_exporter.sqla.SqlaModelBuilder.to_dict. Captures the components
        of a database table schemas: col names, types, foreign keys, etc
        :type orm_models_dict: dict
        :param namespace: the Avro schema namespace
        :type namespace: str
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.orm_models_dict = orm_models_dict
        self.namespace = namespace
        self.data = self.build()

    def build(self):
        """
        Build a dict representing the Metadata PFB Entity.

        Note on namespace
        -----------------
        The metadata object is wrapped in a tuple before it's added to the
        PFB Entity dict. The first value in the tuple identifies the PFB Entity
        avro schema that will be used when reading this object out of the
        Avro file.

        See pfb_exporter.config.METADATA_TEMPLATE for dict structure
        """
        self.logger.info('Building Metadata PFB Entity')

        with open(os.path.join(METADATA_TEMPLATE), 'r') as json_file:
            metadata_templates = json.load(json_file)

        metadata = metadata_templates['metadata']
        metadata['id'] = 'Metadata'

        for table_name, model_dict in self.orm_models_dict.items():
            node = deepcopy(metadata_templates['node'])
            node['name'] = table_name

            for prop in model_dict['properties']:
                property = deepcopy(metadata_templates['property'])
                property['name'] = prop['name']
                node['properties'].append(property)

            for fk in model_dict['foreign_keys']:
                link = deepcopy(metadata_templates['link'])
                link['dst'] = fk['table']
                link['multiplicity'] = 'MANY_TO_ONE'
                link['name'] = link['dst']
                node['links'].append(link)

            metadata['object']['nodes'].append(node)

        metadata['object'] = (
            f'{self.namespace}.{metadata["name"]}', metadata['object']
        )

        return metadata


class PfbTableRowEntity(object):
    def __init__(self, idx, row_dict, orm_model_dict, namespace):
        """
        Builds the Table Row PFB Entity, a dict which captures a row of data
        from a particular table in the database

        :param idx: sequential index of the row
        :type idx: int
        :param row_dict: Key, value representing a row of data from a database
        table. The key is the column name, and value is the column value
        This is used as the PFB Entity's object
        :type row_dict: dict
        :param orm_model_dict: a value from the dict output of
        pfb_exporter.sqla.SqlaModelBuilder.to_dict. Captures the components of
        a database table's schema: col names, types, foreign keys, etc
        :type orm_model_dict: dict
        :param namespace: the Avro schema namespace
        :type namespace: str
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.idx = idx
        self.row_dict = row_dict
        self.orm_model_dict = orm_model_dict
        self.namespace = namespace
        self.data = self.build()

    def build(self):
        """
        Create a PFB Entity from a row of data in a database table

        Note on namespace
        -----------------
        The row_dict object is wrapped in a tuple before it's added to the
        PFB Entity dict. The first value in the tuple identifies the PFB Entity
        avro schema that will be used when reading this row_dict out of the
        Avro file.

        See pfb_exporter.config.ENTITY_TEMPLATE for dict structure
        """
        with open(ENTITY_TEMPLATE, 'r') as json_file:
            entity_template = json.load(json_file)

        id_ = self.row_dict.get(self.orm_model_dict['primary_key'])
        table_name = self.orm_model_dict['table_name']

        self.logger.info(
            f'Building Table Row PFB Entity #{self.idx} for '
            f'{table_name}: {id_}'
        )

        # Create relations for foreign keys
        fk_schema_dict = {
            fk['fkname']: fk
            for fk in self.orm_model_dict['foreign_keys']
        }
        relations = []
        for col, value in self.row_dict.items():
            if col in fk_schema_dict and value:
                relations.append(
                    {
                        'dst_id': value,
                        'dst_name': fk_schema_dict.get(col, {}).get('table')
                    }
                )

        # Create namespace
        namespace = f'{self.namespace}.{table_name}'

        # Populate entity dict
        entity_template.update({
            'id': id_,
            'name': table_name,
            'object': (namespace, self.row_dict),
            'relations': relations
        })

        self.logger.debug(f'\n{entity_template}')

        return entity_template
