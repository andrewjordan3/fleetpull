"""Suite-wide fixtures.

``setup_logger`` mutates global state (the ``'fleetpull'`` logger:
handlers, level, ``propagate = False``), and any test that exercises a
path through it -- ``Sync.run()`` most prominently -- would otherwise
poison every later ``caplog``-based test in the session: with
propagation off, package records never reach the root logger, so caplog
captures nothing. The autouse snapshot/restore below makes logger state
test-local for the whole suite (promoted from the module-local fixture
``tests/logger/test_setup.py`` carried for exactly this hazard).
"""

import logging
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def restore_package_logger() -> Iterator[None]:
    package_logger = logging.getLogger('fleetpull')
    saved_handlers = list(package_logger.handlers)
    saved_level = package_logger.level
    saved_propagate = package_logger.propagate
    yield
    # Close handlers a test attached (an unclosed FileHandler holds a
    # lock on its tmp_path file on Windows), then restore the snapshot.
    for attached_handler in package_logger.handlers:
        if attached_handler not in saved_handlers:
            attached_handler.close()
    package_logger.handlers = saved_handlers
    package_logger.setLevel(saved_level)
    package_logger.propagate = saved_propagate
