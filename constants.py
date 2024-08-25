import os
from datetime import datetime as dt

FLOAT_FIELDS_PRICES= ["OPEN", "HIGH" ,"LOW", "CLOSE", "adjClose"]
DATA_START_DATE = dt(2000, 1, 1)
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


FINANCIALS_TO_PROCESS = {"revenue": "revenuefromcontractwithcustomerexcludingassessedtax",
                         "shareholders_equity":"stockholdersequity",
                         "cash_flow_from_operations":"netcashprovidedbyusedinoperatingactivities"}