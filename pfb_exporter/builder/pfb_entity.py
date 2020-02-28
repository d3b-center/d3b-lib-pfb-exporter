"""
Contains helper class to transform data conforming to SQLAlchemy model into
PFB Entities which conform to PFB Schema
"""
import os
import json
import logging
from copy import deepcopy
from itertools import chain

from pfb_exporter.config import METADATA_TEMPLATE


class PfbEntityBuilder(object):
    def __init__(self, relational_model, data_dir, output_dir):
        self.logger = logging.getLogger(type(self).__name__)
        self.relational_model = relational_model
        self.output_dir = output_dir
        self.metadata_file = os.path.join(self.output_dir, 'metadata.json')

    def create(self):
        """
        Create and yield PFB Entity dicts

        - Metadata Entity: captures graph structure
        - Other Entities: each represents a row from a database table
        """
        return chain(
            [self._create_metadata()],
            self._yield_entities()
        )

    def _yield_entities(self):
        for fn in os.listdir(self.data_dir):
            with open(os.path.join(self.data_dir, fn), 'r') as json_file:
                yield self._create_entity(json.load(json_file))

    def _create_metadata(self):
        """
        Create Metadata dict representing the PFB graph structure
        """
        with open(os.path.join(METADATA_TEMPLATE), 'r') as json_file:
            metadata_templates = json.load(json_file)

        metadata = deepcopy(metadata_templates['metadata'])

        for model_dict in self.relational_model:
            node = deepcopy(metadata_templates['node'])
            node['name'] = model_dict['table_name']

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

        with open(self.metadata_file, 'w') as json_file:
            json.dump(metadata, json_file, indent=4, sort_keys=True)

        return metadata

    def _create_entity(self, entity_dict):
        """
        Create PFB Entity representing a row from a database table
        """
        return {}
