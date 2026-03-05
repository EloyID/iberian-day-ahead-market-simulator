import os

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
PARTICIPANTS_BIDDING_ZONES_FILEPATH = os.path.normpath(
    os.path.join(PACKAGE_DIR, "data", "participants_bidding_zones.csv")
)
