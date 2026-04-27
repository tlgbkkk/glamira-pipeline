import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "countly"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IP2LOC_DB_PATH = os.path.join(BASE_DIR, "data", "ip_geo", "IP-COUNTRY-REGION-CITY.BIN")