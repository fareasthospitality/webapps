###################
# Purpose: Windows Task Scheduler to run this file at specific intervals (eg: every 30 mins). Working assumption = 30 mins.
# All reports to be run, to be put here in 1 place, for ease of maintenance. For errors, look up respective logs.

# Data Access Window. Because SAS server has jobs which MOVE the files, there is a specific agreed data window within
# which to copy the files.
###################
from feh.datareader import OperaDataReader, OTAIDataReader, FWKDataReader, EzrmsDataReader
import datetime as dt

TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

# FWK #
# Data Access Window: 0200-0430 hrs
if dt.time(3, 0) <= TIME_NOW < dt.time(3, 30):
    fwk_dr = FWKDataReader()
    fwk_dr.load()
