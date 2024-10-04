import utils.constant as constant
import os
from utils.insert_sql import DATA
from dotenv import load_dotenv

load_dotenv()

try:
    influx_to_sqlserver = DATA(
        sql_server  = os.getenv('SQL_SERVER'),
        sql_database =os.getenv('SQL_DATABASE'),
        sql_user_login=os.getenv('SQL_USER_LOGIN'),
        sql_password=os.getenv('SQL_PASSWORD'),
        table=os.getenv('TABLE_1'),
        table_columns=os.getenv('DATA_COLUMNS'),
        table_log=os.getenv('TABLE_LOG'),
        table_columns_log=os.getenv('TABLE_COLUMNS_LOG'),
        influx_server=os.getenv('INFLUX_SERVER'),
        influx_database=os.getenv('INFLUX_DATABASE'),
        influx_user_login=os.getenv('INFLUX_USER_LOGIN'),
        influx_password=os.getenv('INFLUX_PASSWORD'),
        influx_columns=os.getenv('INFLUX_COLUMNS'),
        influx_port = os.getenv('INFLUX_PORT'),
        mqtt_topic=os.getenv('MQTT_TOPIC1'),
        initial_db=os.getenv('SQL_INITIAL_DB')
    )
    influx_to_sqlserver.run()

except Exception as e:
    print(e)