import logging

from zpz.logging import config_logger

logger = logging.getLogger(__name__)


# This test does not assert any result.
# It will pass as long as the code does not crash.
# To see the printout, run with `py.test -s`.
def test_logging():
    config_logger(level='warning', with_thread_name=True)
    logger.debug('debug info')
    logger.info('some info')
    logger.warning('warning!')
    logger.error('something is wrong!')
    logger.critical('something terrible has happened! omg omg omg OMG OMG OMG next line OMG next line OMG yes go to next line\nOMG OMG')

