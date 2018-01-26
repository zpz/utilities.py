import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
""" Set default logging handler to avoid "No handler found" warnings."""