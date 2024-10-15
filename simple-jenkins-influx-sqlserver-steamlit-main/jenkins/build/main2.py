import utils.constant as constant
import os
from utils.influx_to_sqlserver import DATA
from dotenv import load_dotenv

load_dotenv()

try:
    influx_to_sqlserver = DATA(
        sql_server  = os.getenv('SQL_SERVER'),
        sql_database =os.getenv('SQL_DATABASE'),
        sql_user_login=os.getenv('SQL_USER_LOGIN'),
        sql_password=os.getenv('SQL_PASSWORD'),

        table_1=os.getenv('TABLE_1'),
        table_columns_1=os.getenv('TABLE_COLUMNS_1'),
        table_log_1=os.getenv('TABLE_LOG_1'),
        table_columns_log_1=os.getenv('TABLE_COLUMNS_LOG'),

        table_2=os.getenv('TABLE_2'),
        table_columns_2=os.getenv('TABLE_COLUMNS_2'),
        table_log_2=os.getenv('TABLE_LOG_2'),
        table_columns_log_2=os.getenv('TABLE_COLUMNS_LOG'),

        influx_server=os.getenv('INFLUX_SERVER'),
        influx_database=os.getenv('INFLUX_DATABASE'),
        influx_user_login=os.getenv('INFLUX_USER_LOGIN'),
        influx_password=os.getenv('INFLUX_PASSWORD'),
        influx_port = os.getenv('INFLUX_PORT'),

        influx_columns_1=os.getenv('INFLUX_COLUMNS_1'),
        mqtt_topic_1=os.getenv('MQTT_TOPIC_1'),
        influx_columns_2=os.getenv('INFLUX_COLUMNS_2'),
        mqtt_topic_2=os.getenv('MQTT_TOPIC_2'),
        initial_db=os.getenv('SQL_INITIAL_DB')
    )

    influx_to_sqlserver.run2() #output

except Exception as e:
    print(e)