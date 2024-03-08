import os
import sys
import logging
import psycopg2
import psycopg2.extras as extras
from datetime import datetime
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, errorcodes, errors #error handling libraries for psycopg2

logger = logging.getLogger(__name__)

# function that handles and parses psycopg2 exceptions
def show_psycopg2_exception(err):
    try:
        err_type, err_obj, traceback = sys.exc_info()
        
        line_n = traceback.tb_lineno 
        
        print ("\npsycopg2 ERROR:", err, "on line number:", line_n) 
        print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
        
        print ("\nextensions.Diagnostics:", err.diag)
        
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")
        logger.info('Parse psycopg2 exceptions function executed successfully')
    except Exception as e:
        logger.error('Parse psycopg2 exceptions function failed with error: {}'.format(str(e)))


# connect function for PostgreSQL database server
def connect(conn_params_dict):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dict)
        print("Connection successful!")
        logger.info('Connection to PostgreSQL database successful')
    except OperationalError as err:
        show_psycopg2_exception(err) #passing exception to function
        conn = None #set the connection to 'None' in case of error
        logger.info('Connection to PostgreSQL database failed with error')
    return conn