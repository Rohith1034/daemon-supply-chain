import os
from dotenv import load_dotenv

load_dotenv()


# ==========================
# Database Configuration
# ==========================

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 55432)),
    "database": os.getenv("DB_NAME", "supply_chain"),
    "user": os.getenv("DB_USER", "supplychain_app"),
    "password": os.getenv("DB_PASSWORD", "Cr7@1034"),
}


# ==========================
# Application Configuration
# ==========================

APP_NAME = "Daemon Supply Chain Simulator"

TIMEZONE = "UTC"

DEFAULT_CURRENCY = "USD"


# ==========================
# Event Configuration
# ==========================

EVENT_STATUS_PENDING = "PENDING"


# ==========================
# Purchase Order Rules
# ==========================

MIN_PO_PRODUCTS = 3
MAX_PO_PRODUCTS = 8

MIN_PO_QUANTITY = 5
MAX_PO_QUANTITY = 60


# ==========================
# Shipment Rules
# ==========================

MIN_SHIPMENT_DAYS = 1
MAX_SHIPMENT_DAYS = 7


# ==========================
# Inventory Rules
# ==========================

MIN_SAFETY_STOCK = 20
MAX_SAFETY_STOCK = 200


# ==========================
# ID Configuration
# ==========================

ID_PADDING_LENGTH = 9