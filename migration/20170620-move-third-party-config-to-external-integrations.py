#!/usr/bin/env python
"""Move integration details from the Configuration file into the
database as ExternalIntegrations
"""
import os
import sys
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.config import Configuration
from core.model import (
    ExternalIntegration as EI,
    get_one_or_create,
    production_session,
)

log = logging.getLogger(name="Core configuration import")

def log_import(integration_or_setting):
    log.info("CREATED: %r" % integration_or_setting)


_db = production_session()
try:
    Configuration.load()

    shadowcat_conf = Configuration.integration('Shadowcat')
    if shadowcat_conf and shadowcat_config.get('url'):
        shadowcat = EI(name=EI.NYPL_SHADOWCAT, goal=EI.METADATA_GOAL)
        _db.add(shadowcat)
        shadowcat.url = shadowcat_config.get('url')
        log_import(shadowcat)

    content_cafe_conf = Configuration.integration('Content Cafe')
    if content_cafe_conf:
        content_cafe = EI(name=EI.CONTENT_CAFE, goal=EI.METADATA_GOAL)
        _db.add(content_cafe)

        content_cafe.username = content_cafe_conf.get('username')
        content_cafe.password = content_cafe_conf.get('password')

    


finally:
    _db.commit()
    _db.close()
