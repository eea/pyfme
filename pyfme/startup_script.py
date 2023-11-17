import fme
import fmeobjects
from datetime import datetime
import os
import uuid

logger = fmeobjects.FMELogFile()
local = False

# Evaluate if we are executing on the server or on desktop, when running on FME server we must have an FME_ENGINE alocated.
fmeengine = ""
try:
    fmeengine = fme.macroValues["FME_ENGINE"]
except:
    pass

if fmeengine == "":
    local = True
else:
    logger.logMessageString(
        f"Running on FME Server at with engine location: '{fmeengine}'."
    )

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

import warnings

with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=UserWarning)

# Generate timestamp
dt = datetime.now()
str_dt = dt.strftime("%Y%m%d%H%M")

uuid_str = str(uuid.uuid4())

zip_filename = "nitrate_" + str_dt
if local:
    output_temp_directory = os.path.join(
        r"C:\Users", os.getlogin(), r"AppData\Local\Temp", uuid_str, zip_filename
    )
    output_zip_filepath = os.path.join(
        r"C:\Users",
        os.getlogin(),
        r"AppData\Local\Temp",
        uuid_str,
        zip_filename + ".zip",
    )

else:
    output_temp_directory = os.path.join(
        FME_MacroValues["FME_SHAREDRESOURCE_TEMP"], uuid_str, zip_filename
    )
    output_zip_filepath = os.path.join(
        FME_MacroValues["FME_SHAREDRESOURCE_TEMP"], uuid_str, zip_filename + ".zip"
    )
try:
    logger.logMessageString(
        f"Creating output directory created: '{output_temp_directory}'."
    )
    os.makedirs(output_temp_directory)
except FileExistsError:
    # directory already exists
    pass


# If we are on the server we expect to find the content on a specific place on the server. If it is on desktop we going to take what is stored inside the inputfile-parameter
if local:
    # We run on desktop
    excel_filepath = FME_MacroValues["excel_filepath"]
else:
    # We run on server
    excel_filepath = FME_MacroValues["inputfile_auto"]

# excel_filepath = r"C:\Users\ridler\nitrates\fake_italy_reporting.xlsx"
if not os.path.exists(excel_filepath):
    logger.logMessageString(f"Excel input file not found at: '{excel_filepath}'.")
    raise FileNotFoundError(f"Error. Excel input file not found {excel_filepath}")


try:
    import fme
    import fmeobjects

except:
    pass

# Evaluate if we are executing on the server or on desktop, when running on FME server we must have an FME_ENGINE alocated.
fmeengine = ""
local = False
try:
    fmeengine = fme.macroValues["FME_ENGINE"]
except:
    pass

if fmeengine == "":
    local = True


import sys
import os
from xmlrpc.client import Boolean
from zipfile import ZipFile

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


if not os.path.isdir(python_directory):
    raise FileNotFoundError(
        f"Error. Python library directory not found at {python_directory}"
    )

sys.path.append(python_directory)


import pandas as pd
import requests

from datetime import datetime
from typing import Dict, List, Tuple, Union, Optional
import numpy as np
from zipfile import ZipFile
