from sqlalchemy import create_engine 
from dotenv import load_dotenv
import os

# database connection parameters
load_dotenv()
HOST_NAME = os.getenv('HOST_NAME')
ELECTRICITY_DATABASE_NAME = os.getenv('ELECTRICITY_DATABASE_NAME')
USER_NAME = os.getenv('USER_NAME')
DB_KEY = os.getenv('ELECTRICITY_DATABASE_KEY')

def connect_db():
    """
    Load environment variables and open a connection to the PostgreSQL database.

    Reads the following environment variables (via python-dotenv):
      - HOST_NAME
      - ELECTRICITY_DATABASE_NAME
      - USER_NAME
      - ELECTRICITY_DATABASE_KEY

    Returns:
      psycopg2.cursor: A cursor object for executing SQL statements.

    Notes:
      - This function calls load_dotenv() so a .env file will be loaded if present.
      - The underlying connection object is created but not returned; the caller
        is responsible for committing and closing the connection if needed.
      - psycopg2 exceptions (e.g. OperationalError) will propagate on failure.
    """
    engine = create_engine(f"postgresql+psycopg2://{USER_NAME}:{DB_KEY}@{HOST_NAME}/{ELECTRICITY_DATABASE_NAME}")
    
    return engine