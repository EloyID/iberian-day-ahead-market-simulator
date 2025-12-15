import os

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
UOF_ZONES_FILEPATH = os.path.normpath(
    os.path.join(PACKAGE_DIR, "data", "uof_zones.csv")
)
