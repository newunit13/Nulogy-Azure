import pyodbc 
from typing import List
from utils.config import DAX_DB_CONNECTION_STRING, AZURE_DB_CONNECTION_STRING


def query(query: str, azure: bool=False) -> List[tuple]:

    cnxn = pyodbc.connect(DAX_DB_CONNECTION_STRING) if not azure else pyodbc.connect(AZURE_DB_CONNECTION_STRING)
    cursor = cnxn.cursor()

    cursor.execute(query)

    result = cursor.fetchall()
    cnxn.close()

    return result

def insert(table: str, record: str, azure: bool=False) -> None:

    cnxn = pyodbc.connect(DAX_DB_CONNECTION_STRING) if not azure else pyodbc.connect(AZURE_DB_CONNECTION_STRING)
    cursor = cnxn.cursor()

    field_headers = []

    cursor.execute(f"INSERT INTO [{table}] VALUES ({record})")
    cursor.commit()

    cnxn.close()

def update(table: str, key_column: str, key_value: str, record: List[tuple], azure: bool=False) -> None:

    cnxn = pyodbc.connect(DAX_DB_CONNECTION_STRING) if not azure else pyodbc.connect(AZURE_DB_CONNECTION_STRING)
    cursor = cnxn.cursor()

    statement = f"""
UPDATE {table}
SET {', '.join([f"[{column}] = '{value}'" for column, value in record])}
WHERE [{key_column}] = '{key_value}'
    """

    cursor.execute(statement)
    cursor.commit()

    cnxn.close()
