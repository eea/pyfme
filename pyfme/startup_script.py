try:
    import fme
    import fmeobjects

except:
    pass

# Evaluate if we are executing on the server or on desktop, when running on FME server we must have an FME_ENGINE alocated.
fmeengine = ""
local = False
logger = None
try:
    fmeengine = fme.macroValues["FME_ENGINE"]
    logger = fmeobjects.FMELogFile()
    logger.logMessageString(
        f"Running on FME Server at with engine location: '{fmeengine}'."
    )
except:
    print("Running Outside of the FME Server Context")
    pass

if fmeengine == "":
    local = True

# IMPORTS Part of the FME
import sys
import os
from zipfile import ZipFile
from datetime import datetime
import os
import uuid

######################################################
python_directory = ""
if local:
    python_directory = os.path.join(
        r"C:\Users", os.getlogin(), r"Documents\FME\Plugins\Python"
    )
else:
    python_directory = (
        FME_MacroValues["FME_SHAREDRESOURCE_DATA"]
        + r"Nitrate/Engine/Plugins/Python/Python"
    )
    logger.logMessageString(f"Python dirctory set to: '{python_directory}'.")

if not os.path.isdir(python_directory):
    raise FileNotFoundError(
        f"Error. Python library directory not found at {python_directory}"
    )
sys.path.append(python_directory)

import warnings

with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=UserWarning)

######################################################

pyfme_directory = ""
pyfme_version = "v0.0.4"

if local:
    pyfme_directory = os.path.join(
        r"C:\Users",
        os.getlogin(),
        r"Documents\FME\Plugins\pyfme_directory",
        pyfme_version,
    )
else:
    pyfme_directory = os.path.join(
        FME_MacroValues["FME_SHAREDRESOURCE_DATA"], "pyfme", pyfme_version
    )

logger.logMessageString(f"pyfme dirctory set to: '{pyfme_directory}'.")

if not os.path.isdir(pyfme_directory):
    import requests

    os.makedirs(pyfme_directory)
    pyfme_zipfile = os.path.join(pyfme_directory, pyfme_version + ".zip")
    url = r"https://github.com/eea/pyfme/archive/refs/tags/" + pyfme_version + ".zip"
    r = requests.get(url, allow_redirects=True)
    with open(pyfme_zipfile, "wb") as f:
        f.write(r.content)
    archive = ZipFile(pyfme_zipfile)

    for file in archive.namelist():
        if file.startswith("pyfme-" + pyfme_version.replace("v", "") + r"/pyfme"):
            print(file)
            archive.extract(file, os.path.join(pyfme_directory))

    pyfme_directory = os.path.join(
        pyfme_directory, "pyfme-" + pyfme_version.replace("v", "")
    )
    sys.path.append(pyfme_directory)

else:
    pyfme_directory = os.path.join(
        pyfme_directory, "pyfme-" + pyfme_version.replace("v", "")
    )
    sys.path.append(pyfme_directory)

import pandas as pd
from pyfme import Xlsx
