from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
import os
import urllib
from dotenv import load_dotenv
from typing import Callable
from sqlalchemy.engine import Engine

load_dotenv()


class Db:
    def __init__(self):
        self.cnxn = os.getenv("DB_CONNECTION")

    def _create_engine(self) -> Engine:
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(self.cnxn)}"
        )
        return engine


database = Db()
sessionfac = sessionmaker(bind=database._create_engine())
session = sessionfac()
start_time = datetime.now()
query = session.execute(
    text(
        "Select DetailKey,JobName from Hourlytime LEFT JOIN Job on Hourlytime.JobKey = Job.JobKey"
    )
)
result_as_dict = query.mappings().all()
for row in result_as_dict:
    print(row)
elapsed_time = datetime.now() - start_time
print(f"Elapsed time: {elapsed_time}")
