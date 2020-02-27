"""
Contains helper class to transform data conforming to SQLAlchemy model into
PFB Entities which conform to PFB Schema
"""
import logging


class PfbEntityBuilder(object):
    def __init__(self, model_dict, output_dir):
        self.logger = logging.getLogger(type(self).__name__)
        self.model_dict = model_dict
        self.output_dir = output_dir
