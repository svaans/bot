import os
from dotenv import load_dotenv

load_dotenv()

MODO_REAL = os.getenv("MODO_REAL", "False").lower() == "true"
