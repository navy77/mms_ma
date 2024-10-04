import utils.constant as constant
import pandas as pd
import os
import sys
import pymssql
import urllib.parse
from sqlalchemy import create_engine,text,engine
from influxdb import InfluxDBClient
from datetime import datetime

class PREPARE:

    def __init__(self,sql_server,sql_database,sql_user_login,sql_password,table,table_columns,
    table_log,table_columns_log,influx_server,influx_database,influx_user_login,influx_password,
    influx_columns,influx_port,mqtt_topic,initial_db):
        self.sql_server = sql_server
        self.sql_database = sql_database
        self.sql_user_login = sql_user_login
        self.sql_password = sql_password
        self.table = table
        self.table_columns = table_columns
        self.table_log = table_log
        self.table_columns_log = table_columns_log
        self.influx_server = influx_server
        self.influx_database = influx_database
        self.influx_user_login = influx_user_login
        self.influx_password = influx_password
        self.influx_columns = influx_columns
        self.influx_port = influx_port
        self.mqtt_topic = mqtt_topic
        self.initial_db = initial_db
        self.df_insert = None
        self.df_influx = None
        self.df_sql = None
        
    def stamp_time(self):
        now = datetime.now()
        print("\nHi this is job run at -- %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))

    def error_msg(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"process":process,"message":msg,"error":e}

        try:
            # self.log_to_db(result)
            sys.exit()
        except Exception as e:
            self.info_msg(self.error_msg.__name__,e)
            sys.exit()
                
    def info_msg(self,process,msg):
        result = {"status":constant.STATUS_INFO,"process":process,"message":msg,"error":"-"}
        print(result)

    def ok_msg(self,process):
        result = {"status":constant.STATUS_OK,"process":process,"message":"program running done","error":"-"}
        try:
            # self.log_to_db(result)
            print(result)
        except Exception as e:
            self.error_msg(self.ok_msg.__name__,'cannot ok msg to log',e)
    
    def conn_sql(self):
        try:
            cnxn = pymssql.connect(self.sql_server, self.sql_user_login, self.sql_password, self.sql_database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.info_msg(self.conn_sql.__name__,e)
            sys.exit()

    def log_to_db(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table_log}] 
                values(
                    getdate(), 
                    '{result["status"]}', 
                    '{result["process"]}', 
                    '{result["message"]}', 
                    '{str(result["error"]).replace("'",'"')}'
                    )
                    """
                )
            cnxn.commit()
            cursor.close()
        except Exception as e:
            self.alert_line("Danger! cannot insert log table")
            self.info_msg(self.log_to_db.__name__,e)
            sys.exit()

class DATA(PREPARE):

    def __init__(self,sql_server,sql_database,sql_user_login,sql_password,table,table_columns,table_log,table_columns_log,influx_server,influx_database,influx_user_login,influx_password,influx_columns,influx_port,mqtt_topic,initial_db):
        super().__init__(sql_server,sql_database,sql_user_login,sql_password,table,table_columns,table_log,table_columns_log,influx_server,influx_database,influx_user_login,influx_password,influx_columns,influx_port,mqtt_topic,initial_db)      
    
    def query_influx(self) :
        try:
            client = InfluxDBClient(host=self.influx_server,port=self.influx_port,username=self.influx_user_login,password=self.influx_password,database=self.influx_database)
            query = f"select * from mqtt_consumer where topic =~ {self.mqtt_topic} order by time desc limit 20"
            result = client.query(query)
            result_df = pd.DataFrame(result.get_points())
            df = result_df.copy()
            df_split = df['topic'].str.split('/', expand=True)
            df['equipment_no'] = df_split[3]
            df["time"] =   pd.to_datetime(df["time"]).dt.tz_convert(None)
            df["time"] = df["time"] + pd.DateOffset(hours=7)    
            df["time"] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')[:-3])
            if not df.empty :
                self.df_influx = df
            else:
                self.df_influx = None
                self.info_msg(self.query_influx.__name__,"influxdb data is emply")
        except Exception as e:
            self.error_msg(self.query_influx.__name__,"cannot query influxdb",e)

    def query_sql(self):
        try:
            encoded_password = urllib.parse.quote_plus(self.sql_password)
            engine = create_engine(f'mssql+pymssql://{self.sql_user_login}:{encoded_password}@{self.sql_server}/{self.sql_database}')
            sql_query = f"""SELECT TOP 100 * FROM [{self.sql_database}].[dbo].[{self.table}] ORDER by time desc"""
            print(sql_query)
            df_sql = pd.read_sql(sql_query, engine)
            columns = df_sql.columns.tolist()
            self.df_sql = df_sql[columns]
            if self.df_sql.empty :
                self.info_msg(self.query_sql.__name__,f"data is emply")
            return self.df_sql
        except Exception as e:
                self.error_msg(self.query_sql.__name__,"cannot select with sql code",e)
 
    def check_duplicate(self):
        try:
            df_from_influx = self.df_influx
            df_from_sql = self.df_sql 
            df_from_sql["time"] = df_from_sql['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')[:-3])
            df_not_duplicate = df_from_influx[~df_from_influx[['time', 'spec_id']].apply(tuple, 1).isin(df_from_sql[['time', 'spec_id']].apply(tuple, 1))]
            # print(df_not_duplicate)
            if df_not_duplicate.empty:    
                self.df_insert=None     
                self.info_msg(self.check_duplicate.__name__,f"data is not new for update")
            else:
                self.info_msg(self.check_duplicate.__name__,f"we have data new")
                self.df_insert = df_not_duplicate    
                return constant.STATUS_OK   
        except Exception as e:
            self.error_msg(self.check_duplicate.__name__,"cannot select with sql code",e)
    
    def df_to_db(self):
        insert_db_value = self.table_columns.split(",")
        col_list = insert_db_value
        cnxn,cursor=self.conn_sql()
        try:
            if not self.df_insert is None:  
                df = self.df_insert
                df = df[col_list]
                for index, row in df.iterrows():
                    value = None
                    for i in range(len(col_list)):
                        address = col_list[i]
                        if value == None:
                            value = "'"+str(row[address])+"'"
                        else:
                            value = value+",'"+str(row[address])+"'"
                    insert_string = f"""
                    INSERT INTO [{self.database}].[dbo].[{self.table}] 
                    values(
                        {value}
                        )
                        """
                    cursor.execute(insert_string)
                    cnxn.commit()
                cursor.close()
                self.df_insert = None

                self.info_msg(self.df_to_db.__name__,f"insert data successfully")     
        except Exception as e:
            print('error: '+str(e))
            self.error_msg(self.df_to_db.__name__,"cannot insert df to sql",e)

    def run(self):
        self.stamp_time()
        if self.initial_db == 'True':
            self.query_influx()
            if self.df_influx is not None:
                self.query_sql()
            #     if self.df_sql.empty:
            #         self.df_insert = self.df_influx
            #         self.df_to_db()
            #         self.ok_msg(self.df_to_db.__name__)
            #     else:
            #         self.check_duplicate()
            #         if self.df_insert is not None:
            #             self.df_to_db()
            #             self.ok_msg(self.df_to_db.__name__)
        else:
            print("db is not initial yet")

if __name__ == "__main__":
    print("must be run with main")