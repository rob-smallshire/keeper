import logging
import sys

logging.basicConfig(level=logging.DEBUG)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.DEBUG)
# add the handler to the root logger
logging.getLogger().addHandler(console)
