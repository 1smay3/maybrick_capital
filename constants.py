import os

FLOAT_FIELDS_PRICES= ["OPEN", "HIGH" ,"LOW", "CLOSE", "adjClose"]
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


FINANCIALS_TO_PROCESS = {"revenue": "revenuefromcontractwithcustomerexcludingassessedtax",
                         "shareholders_equity":"stockholdersequity",
                         "cash_flow_from_operations":"netcashprovidedbyusedinoperatingactivities"}