import logging
import os

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DEFAULT_LOG_LEVEL = logging.DEBUG
DEFAULT_LOG_OVERWRITE_OPT = True
DEFAULT_LOG_FILENAME = "pfb_export.log"

DEFAULT_OUTPUT_DIR = os.path.join(os.getcwd(), 'pfb_export')

DEFAULT_PFB_FILE = 'pfb.avro'
DEFAULT_PFB_SCHEMA_FILE = 'pfb_schema.json'
REL_MODEL_FILE = 'relational_model.json'
PFB_SCHEMA_TEMPLATE = os.path.join(
    ROOT_DIR, 'template', DEFAULT_PFB_SCHEMA_FILE
)
METADATA_TEMPLATE = os.path.join(
    ROOT_DIR, 'template', 'metadata.json'
)
DEFAULT_MODELS_PATH = os.path.join(DEFAULT_OUTPUT_DIR, 'models.py')
