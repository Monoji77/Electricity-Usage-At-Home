from datetime import datetime, timedelta
from random import randint, seed
from dotenv import load_dotenv
import os
import logging
import sqlalchemy
from sqlalchemy import text
import pandas as pd

from utilities.connect_to_database import connect_db

############################
# 
# (1) GLOBAL VARIABLE
#
############################

# database connection parameters
load_dotenv()
HOST_NAME = os.getenv('HOST_NAME')
ELECTRICITY_DATABASE_NAME = os.getenv('ELECTRICITY_DATABASE_NAME')
USER_NAME = os.getenv('USER_NAME')
DB_KEY = os.getenv('ELECTRICITY_DATABASE_KEY')

# destination table column names
ELECTRICITY_CONSUMPTION_TABLE_NAME = 'electricity_consumption'
APPLIANCE_NAME = 'appliance_name'
POWER_CONSUMPTION_VAL =  'power_consumption_val' 
RECORDED_AT_TIMESTAMP = 'recorded_at_timestamp'
column_names_sql = ', '.join([APPLIANCE_NAME, POWER_CONSUMPTION_VAL, RECORDED_AT_TIMESTAMP])    

# logger counter
i = 1

############################
# 
# (2) HELPER FUNCTION 
#
############################    

def insert_into_db(appliance_name:str, 
                         power:int, 
                         now:datetime, 
                         logger: logging.Logger,
                         engine: sqlalchemy.engine.base.Engine) -> None:
    """
    Build an INSERT SQL statement for a given appliance and a power draw.

    The function applies a small random variation (±5%) to the provided
    nominal power, prints a short message to stdout describing the insert,
    and returns a SQL string that inserts (appliance_name, power, NOW())
    into the configured electricity table.

    Parameters:
      appliance_name (str): human-readable name of the appliance.
      power (int): nominal power rating in watts.
      now (datetime): current timestamp used for the printed message.
      logger (logging.Logger): logger instance 

    Returns:
      str: SQL INSERT statement string ready to be executed by a cursor.
    """
    # 5% randomness in power reading to simulate real life situations 
    seed()
    variation = 0.05 
    lower_bound = round(power*(1-variation))
    upper_bound = round(power*(1+variation))
    final_power = randint(lower_bound, upper_bound)
    
    # print statement
    printed_time = datetime.strftime(now, '%Y-%m-%d %H:%M:%S')
    
    global i
    
    logger.info(f'({i}) INSERTED {appliance_name} into DB with power {final_power}W at {printed_time}')
    i += 1
    params = {
        'appliance_name': appliance_name,
        'final_power': final_power,
    }
    sql_text = text(f"""
        INSERT INTO {ELECTRICITY_CONSUMPTION_TABLE_NAME} ({column_names_sql}) 
        VALUES (:appliance_name, :final_power, NOW());
      """)
    with engine.connect() as conn:
      conn.execute(sql_text, params)
      conn.commit()
    
def simulate_refrigerator_usage(engine: sqlalchemy.engine.base.Engine, 
                                 now:datetime, 
                                 logger: logging.Logger) -> None:
    """
    Simulate a refrigerator power reading and insert it into the database.

    This always emits a single refrigerator reading using a nominal rating
    and executes the resulting INSERT statement with the provided cursor.

    Parameters:
      cur (psycopg2.extensions.cursor): database cursor used to execute SQL.
      now (datetime): current timestamp (used for logging/printing).
      logger (logging.Logger): logger instance for logging errors/info.
    """
    refrigerator_rating = ("Refrigerator", 300, now, logger, engine)
    insert_into_db(*refrigerator_rating)  

def simulate_airconditioner_usage(engine: sqlalchemy.engine.base.Engine, 
                                  now:datetime,
                                  logger: logging.Logger) -> None:
    """
    Simulate air conditioner usage and insert readings when active.

    Air conditioners are simulated to run during early morning or late
    evening/night. When active, this inserts one reading per configured
    AC appliance.

    Parameters:
      cur (psycopg2.extensions.cursor): database cursor used to execute SQL.
      now (datetime): current timestamp used to decide activity window.
      logger (logging.Logger): logger instance for logging errors/info.
    """
    def get_boundaries(family_member:str) -> tuple[int, int]:
      seed_suffix = 1 if family_member == "Parents" else 0
      today_seed = int(now.strftime(f'%Y%m%d{seed_suffix}'))
      seed(today_seed)
      if family_member == "Chris":
        morning_upper_bound = randint(9, 12)
        night_upper_bound = randint(16, 20)
      elif family_member == "Parents":
        morning_upper_bound = randint(6, 10)
        night_upper_bound = randint(18, 20)
      return morning_upper_bound, night_upper_bound
    
    # Chris' Portable AC
    morning_upper_bound, night_upper_bound = get_boundaries("Chris")
    if now.hour <= morning_upper_bound or now.hour >= night_upper_bound:
      insert_into_db("Europace Portable AC", 2100, now, logger, engine)
    
    # Parents' Wall mounted AC
    morning_upper_bound, night_upper_bound = get_boundaries("Parents")
    if now.hour <= morning_upper_bound or now.hour >= night_upper_bound:
      insert_into_db("Wall mounted AC", 3500, now, logger, engine)

def simulate_washing_machine_usage(engine: sqlalchemy.engine.base.Engine, 
                                   now: datetime, 
                                   logger: logging.Logger) -> None:
    """
    Simulate washing machine usage and insert a reading if within the morning window.

    The washing machine is only simulated during a randomized morning window.
    If the current time falls within that window, a single INSERT is executed.

    Parameters:
      cur (psycopg2.extensions.cursor): database cursor used to execute SQL.
      now (datetime): current timestamp used to decide activity window.
      logger (logging.Logger): logger instance for logging errors/info.
    """
    # ensure reproducibility for a same day
    today_seed = int(now.strftime('%Y%m%d'))
    seed(today_seed)
    
    morning_lower_bound = randint(6, 7)
    morning_upper_bound = randint(7,9)
    
    if now.hour >= morning_lower_bound and now.hour <= morning_upper_bound: 
        washing_machine_rating = ("Washing Machine", 1000, now, logger, engine)
        insert_into_db(*washing_machine_rating) 

###################
#
# (3) MAIN 
#
###################

def main():
    """
    Entry point for the simulation script.

    - Configures logging to stdout and a file.
    - Seeds the random generator deterministically by today's date so runs on
      the same day are reproducible.
    - Opens a database cursor and invokes the appliance simulation functions.
    - Logs and re-raises any exception encountered.

    Notes:
      The function expects a working .env configuration for DB connection
      parameters and that the database table matches the expected schema.
    """
    # initiate logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler('././logs/simulation.log'))
    
    try:
        # get current datetime
        
        now_dttm = datetime.now()
        logger.info(f"[{datetime.strftime(now_dttm, '%Y-%m-%d %H:%M:%S')}] Simulation initiated...")
        
        # create cursor object for inserting into database    
        engine = connect_db()
        
        # simulate refrigerator
        simulate_refrigerator_usage(engine, now_dttm, logger)
        
        # simulate airconditioners
        simulate_airconditioner_usage(engine, now_dttm, logger)
        
        # simulate washing machine
        simulate_washing_machine_usage(engine, now_dttm, logger)
        logger.info("Simulation completed successfully.\n")
    except Exception as e:
        logger.error(f"Simulation ended with error: {e}\n")
        raise e

###################
#
# (4) RUN MAIN
#
###################
main()