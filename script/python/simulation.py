from datetime import datetime, timedelta
from random import randint, seed
import psycopg2
from dotenv import load_dotenv
import os
import logging

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
    conn = psycopg2.connect(
        host=HOST_NAME,
        dbname=ELECTRICITY_DATABASE_NAME,
        user=USER_NAME,
        password=DB_KEY,
    )
    
    cur = conn.cursor()
    
    return (conn, cur)

def insert_sql_statement(appliance_name:str, 
                         power:int, 
                         now:datetime, 
                         logger: logging.Logger) -> str:
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
    return f"""
        INSERT INTO {ELECTRICITY_CONSUMPTION_TABLE_NAME} ({column_names_sql})
        VALUES ('{appliance_name}', {final_power}, NOW());     
    """
    
def simulate_refrigerator_usage(cur: psycopg2.extensions.cursor, 
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
    refrigerator_rating = ("Refrigerator", 300, now, logger)
    refrigerator_insert_statement = insert_sql_statement(*refrigerator_rating)
    cur.execute(refrigerator_insert_statement)
    return cur

def simulate_airconditioner_usage(cur: psycopg2.extensions.cursor, 
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
    # ensure reproducibility for same day
    today_seed = int(now.strftime('%Y%m%d'))
    seed(today_seed)
    
    morning_upper_bound = randint(6, 10)
    night_upper_bound = randint(4, 24)

    if now.hour <= morning_upper_bound or now.hour >= night_upper_bound:
        # airconditioners
        airconditioners_dict = {
            "Europace Portable AC": 2100,
            "Wall mounted AC": 3500,
        }
        
        for appliance, power in airconditioners_dict.items():
            insert_statement = insert_sql_statement(appliance, power, now, logger)
            cur.execute(insert_statement)
    return cur

def simulate_washing_machine_usage(cur: psycopg2.extensions.cursor, 
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
        washing_machine_rating = ("Washing Machine", 1000, now, logger)
        washing_machine_insert_statement = insert_sql_statement(*washing_machine_rating) 
        cur.execute(washing_machine_insert_statement)
    return cur


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
        conn, cur = connect_db()
        
        # simulate refrigerator
        cur = simulate_refrigerator_usage(cur, now_dttm, logger)
        
        # simulate airconditioners
        cur = simulate_airconditioner_usage(cur, now_dttm, logger)
        
        # simulate washing machine
        cur = simulate_washing_machine_usage(cur, now_dttm, logger)

        conn.commit()
        cur.close()
        conn.close()
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