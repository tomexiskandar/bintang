import logging
log = logging.getLogger(__name__)
FORMAT = "[%(levelname)s|%(filename)s:%(lineno)s|%(funcName)10s()|%(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.WARNING)
