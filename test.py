import os
import pprint
import sys
import logging
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pandas as pd
# import awswrangler as wr
import datetime
import json

# Initializing the logging format and output
logging_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s'
logger = logging.getLogger(__name__)
logging.basicConfig(format=logging_format)
logger.setLevel(logging.INFO)

# Simple None object
none_object = type(None)()

# Constants
# Environments - dev, stg and prod
_DB_ENV_DEV = "dev"
_DB_ENV_STG = "stg"
_DB_ENV_PROD = "prod"


def get_engine(db_env, db_host, db_port, db_name, db_user_name, db_password):
    """
    Getting the engine object
    :param db_env: Target environment where the database is
    :param db_host: Host of the database
    :param db_port: The database port
    :param db_name: Database name
    :param db_user_name: Database user name
    :param db_password: Database user password
    :return: sqlalchemy connection object
    """
    try:
        logger.info("Creating engine")
        db_url = ""
        if db_env == _DB_ENV_DEV:
            db_url = "mysql+pymysql://" + db_user_name + ":" + db_password + "@" + db_host + "/" + db_name
        elif(db_env == _DB_ENV_STG or db_env == _DB_ENV_PROD):
            db_url = "postgresql+psycopg2://" + db_user_name + ":" + db_password + "@" + db_host + ":" + db_port + "/" + db_name
        else:
            logger.error("The environment is not set")
            sys.exit(1)

        connect_timeout = 350
        engine = sqlalchemy.create_engine(db_url, client_encoding='utf8'
                                          , connect_args=dict(connect_timeout=connect_timeout)
                                          , isolation_level="READ UNCOMMITTED")
        logger.info("Engine is created")
        return engine
    except Exception as e:
        logger.error("Unable to create engine")
        logger.error(e, exc_info=True)
        sys.exit(1)


def get_connection(engine):
    """
    Getting the DB connection object
    :param engine: sqlalchemy engine
    :return: sqlalchemy connection object
    """
    try:
        logger.info("Creating connection")
        connection = engine.connect()
        logger.info("Connection is created")
        return connection
    except Exception as e:
        logger.error("Unable to create connection")
        logger.error(e, exc_info=True)
        sys.exit(1)


def get_metadata(engine, **kwargs):
    """
    Get the metadata object
    :param engine: sqlalchemy engine object
    :return: sqlalchemy metadata object
    """
    try:
        logger.info("Getting the metadata")
        db_environment = kwargs.get("db_environment", None)
        schema = kwargs.get("schema", None)
        metadata = sqlalchemy.MetaData(bind=engine)
        if db_environment == "dev":
            logger.info("Entering into the dev environment metadata creation")
            metadata = sqlalchemy.MetaData(bind=engine)
        elif db_environment == "stg" or db_environment == "prod":
            logger.info("Entering into the stg/prod environment metadata creation")
            metadata = sqlalchemy.MetaData(bind=engine, schema=schema)
        else:
            logger.warning("Some db setting are not set")
        logger.info("Metadata is created")
        return metadata
    except Exception as e:
        logger.error("Unable to get metadata")
        logger.error(e, exc_info=True)
        sys.exit(1)


def get_session(engine):
    """
    Get the session object
    :param engine: sqlalchemy engne object
    :return: sqlalchemy session object
    """
    try:
        logger.info("Getting session")
        Session = sessionmaker(bind=engine)
        session = Session()
        logger.info("Session is created")
        return session
    except Exception as e:
        logger.error("Unable to get session")
        logger.error(e, exc_info=True)
        sys.exit(1)


def get_response(status_code: int, headers: object, content_type: str, body: object, *args, **kwargs) -> object:
    """
    Get response object
    :param status_code: int
    :param headers: object
    :param content_type: str
    :param body: object
    :param args:
    :param kwargs:
    :return: object
    """
    response_object = {}
    response_object["statusCode"] = status_code
    response_object["headers"] = headers
    response_object["headers"]["Content-Type"] = content_type
    response_object["body"] = body
    return response_object


def is_positive(val):
    """
    Validate if input value is positive
    :param val: int, str
    :return: boolean - True/False
    """
    try:
        if (int(val) >= 0):
            return True
        else:
            return False
    except ValueError:
        return False
    return True


def is_int(val):
    """
    Validate if input value can be of integer type
    :param val: int, str
    :return: boolean - True/False
    """
    try:
        num = int(val)
    except ValueError:
        return False
    return True


def is_float(val):
    """
    Validate if input value can be of float type
    :param val: int, float, str
    :return: boolean - True/False
    """
    try:
        num = float(val)
        if (abs(num - int(num)) > 0):
            return True
        else:
            return False
    except ValueError:
        return False
    return True


def validate_parameters(page, size):
    """
    Validate page and size parameters
    :param page: str, int
    :param size: str, int
    :return: True/False
    """
    try:
        if (not int(page) >= 0
                or not is_int(page)
                or not is_int(size)
                or not is_positive(page)
                or not is_positive(size)
                or is_float(page)
                or is_float(size)):
            return False
        else:
            return True
    except ValueError:
        return False
    return True


def check_date(update_date):
    """
    Check the format of the update date
    :param update_date: yyyy-MM-dd hh:mm:ss
    :return: True/False or ValueError
    """
    if update_date != None:
        try:
            if len(update_date) == 10 and datetime.datetime.strptime(update_date, "%Y-%m-%d"):
                return True
            elif len(update_date) == 19 and datetime.datetime.strptime(update_date, "%Y-%m-%d %H:%M:%S"):
                return True
            else:
                raise Exception("Invalid update date string argument =[{0}]".format(update_date))
        except Exception as e:
            logger.error(e, exc_info=False)
            sys.exit(1)
    else:
        return False

def get_date(update_date):
    """
    Gets the formatted and converted from string to datetime update date
    :param update_date: yyyy-MM-dd hh:mm:ss
    :return: datetime or Error
    """
    if update_date != None:
        try:
            if len(update_date) == 10 and datetime.datetime.strptime(update_date, "%Y-%m-%d"):
                update_date_formatted = datetime.datetime.strptime(update_date, "%Y-%m-%d")
                return update_date_formatted
            elif len(update_date) == 19 and datetime.datetime.strptime(update_date, "%Y-%m-%d %H:%M:%S"):
                update_date_formatted = datetime.datetime.strptime(update_date, "%Y-%m-%d %H:%M:%S")
                return update_date_formatted
            else:
               logger.error("Incorrect update date=[{0}] passed".format(update_date))
        except Exception as e:
            logger.error("Invalid update date=[{0}] passed".format(update_date))
            sys.exit(1)


def event_to_dict(event_object):
    """
    Function returning the checked and adjusted event object
    :param event_object:
    :return: returns dict object
    """
    if isinstance(event_object, dict):
        return event_object
    elif isinstance(event_object, str):
        return json.loads(event_object)
    else:
        logger.error("The event object is neither dictionary nor string")
        sys.exit(1)


def lambda_handler(event, context):
    # Initialize the offset and limit values
    status_code = 0
    content_type = ""
    status_message = ""
    event = event_to_dict(event)
    page = event["queryStringParameters"]["page"]
    size = event["queryStringParameters"]["size"]

    if (not validate_parameters(page, size)):
        status_code = 400
        headers = {}
        content_type = "application/json"
        body = {"Page": page, "Size": size}
        response_object = get_response(status_code, headers, content_type, body)
        logger.info("Wrong parameter(s): page=[{0}] size=[{1}]".format(str(page), str(size)))
        return response_object
    else:
        page = int(page)
        size = int(size)
        logger.info("Page=[{0}] and Size=[{1}]".format(str(page), str(size)))

    # Handling update_date_from input
    update_date_from = None
    if "updatedate_from" in event["queryStringParameters"]:
        update_date_from = event["queryStringParameters"]["updatedate_from"]
        if check_date(update_date_from):
            update_date_from = get_date(update_date_from)
            logger.info("Update date from=[{0}]".format(str(update_date_from)))
        else:
            logger.warning("Update date from=[{0}] is incorrect".format(str(update_date_from)))

    # Handling update_date_to input
    update_date_to = None
    if "updatedate_to" in event["queryStringParameters"]:
        update_date_to = event["queryStringParameters"]["updatedate_to"]
        if check_date(update_date_to):
            update_date_to = get_date(update_date_to)
            logger.info("Update date to=[{0}]".format(str(update_date_to)))
        else:
            logger.warning("Update date to=[{0}] is incorrect".format(str(update_date_to)))

    session = None
    engine = None
    db_env = None
    db_schema = None
    metadata = None
    cssp_case_table = None

    try:
        # Initializing the variables and objects
        db_env = os.environ['DB_ENV']
        db_host = os.environ['DB_HOST']
        db_port = os.environ['DB_PORT']
        db_name = os.environ['DB_NAME']
        db_user_name = os.environ['DB_USER_NAME']
        db_password = os.environ['DB_PASSWORD']
        engine = get_engine(db_env=db_env, db_host=db_host, db_port=db_port
                            , db_name=db_name, db_user_name=db_user_name
                            , db_password=db_password)

        session = get_session(engine=engine)
        logger.info("DB parameters and session objects are initialized")

        # Initialize the invoice data table
        case_table = "cssp_case"

        if db_env == _DB_ENV_DEV:
            metadata = get_metadata(engine=engine, db_environment=db_env)
            cssp_case_table = sqlalchemy.Table(case_table, metadata, autoload=True
                                                      , autoload_with=engine)
            logger.info("Input table is initialized")
        elif db_env == _DB_ENV_STG or db_env == _DB_ENV_PROD:
            db_schema = os.environ['DB_SCHEMA']
            if db_schema != None:
                session.execute("SET search_path TO {0}".format(db_schema))
            metadata = get_metadata(engine=engine, db_environment=db_env, schema=db_schema)
            cssp_case_table = sqlalchemy.Table(case_table, metadata, autoload=True
                                                      , autoload_with=engine, schema=db_schema)
            logger.info("Input table is initialized")
        else:
            logger.warning("Other environment set")

        # Construct single dataset
        q = None
        if page != None and size != None and update_date_from == None and update_date_to == None:
            q = session.query(cssp_case_table) \
                .order_by(cssp_case_table.c.id.desc()) \
                .offset(page) \
                .limit(size)
        elif page != None and size != None and update_date_from != None and update_date_to != None:
            q = session.query(cssp_case_table) \
                .filter(cssp_case_table.c.lastmodifieddate >= update_date_from) \
                .filter(cssp_case_table.c.lastmodifieddate <= update_date_to) \
                .order_by(cssp_case_table.c.lastmodifieddate.desc()) \
                .offset(page) \
                .limit(size)
        else:
            logger.info("No query condition was met")

        logger.info("The dataset is constructed")

        # Transform to dataframe, datatypes and return json
        pd_df = pd.read_sql(q.statement, session.bind)
        pd_df = pd_df.astype(str)
        pprint.pprint(pd_df)

    except Exception as e:
        logger.error("Unable to execute the main code")
        logger.error(e, exc_info=True)
        sys.exit(1)
    finally:
        # Closing the session and disposing the engine
        if session is not none_object: session.close()
        if engine is not none_object: engine.dispose()
        logger.info("Closed session and disposed engine")

    # Construct the response_object
    status_code = 200
    headers = {}
    content_type = "application/json"
    body = pd_df.to_json(orient="records")
    response_object = get_response(status_code, headers, content_type, body)
    return response_object
