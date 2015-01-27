#!/usr/bin/env python
"""Generate a dataset suitable for stress-testing the
   permanent ID generation algorithm.
"""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from scripts import PermanentWorkIDStressTestGenerationScript
PermanentWorkIDStressTestGenerationScript(sys.argv[1]).run()
