"""
Contains helper class to transform data conforming to SQLAlchemy model into
PFB Entities which conform to PFB Schema
"""
import logging


class PfbEntityBuilder(object):
    def __init__(self, relational_model, output_dir):
        self.logger = logging.getLogger(type(self).__name__)
        self.relational_model = relational_model
        self.output_dir = output_dir

    def create(self):
        """
        Create PFB Entity JSON objects
        """
        pass
