import os
from dotenv import load_dotenv

load_dotenv("config/claves.env")
MODO_REAL = os.getenv("MODO_REAL", "False").lower() == "true"
