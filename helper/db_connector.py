from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME")

DW_DB_HOST = os.getenv("DW_DB_HOST")
DW_DB_USER = os.getenv("DW_DB_USER")
DW_DB_PASSWORD = os.getenv("DW_DB_PASSWORD")
DW_DB_PORT = os.getenv("DW_DB_PORT")
DW_DB_NAME = os.getenv("DW_DB_NAME")




def source_engine():
    engine = create_engine(f"postgresql://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}")

    return engine


def dw_engine():
    engine = create_engine(f"postgresql://{DW_DB_USER}:{DW_DB_PASSWORD}@{DW_DB_HOST}:{DW_DB_PORT}/{DW_DB_NAME}")

    return engine