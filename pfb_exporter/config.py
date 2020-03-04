import logging
import os

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DEFAULT_LOG_LEVEL = logging.DEBUG
DEFAULT_LOG_OVERWRITE_OPT = True
DEFAULT_LOG_FILENAME = "pfb_export.log"

DEFAULT_OUTPUT_DIR = os.path.join(os.getcwd(), 'pfb_export')
SQLA_MODELS_FILE = 'models.py'
ORM_MODELS_FILE = 'orm_models.json'

DEFAULT_AVRO_SCHEMA_NAMESPACE = 'pfb'
DEFAULT_PFB_FILE = 'pfb.avro'
DEFAULT_PFB_SCHEMA_FILE = 'pfb_schema.json'
DEFAULT_METADATA_FILE = 'metadata.json'

PFB_SCHEMA_TEMPLATE = os.path.join(
    ROOT_DIR, 'template', DEFAULT_PFB_SCHEMA_FILE
)
METADATA_TEMPLATE = os.path.join(
    ROOT_DIR, 'template', 'metadata.json'
)
METADATA_SCHEMA_TEMPLATE = os.path.join(
    ROOT_DIR, 'template', 'metadata_schema.json'
)
ENTITY_TEMPLATE = os.path.join(
    ROOT_DIR, 'template', 'entity.json'
)
