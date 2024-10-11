import utils.constant as constant
import pandas as pd
import os
import sys
import pymssql
import urllib.parse
from sqlalchemy import create_engine,text,engine
from influxdb import InfluxDBClient
import datetime
import time
from dotenv import load_dotenv

load_dotenv()
class PREPARE:

    def __init__(self,sql_server,sql_database,sql_user_login,sql_password,
                 table_1,table_columns_1,table_log_1,table_columns_log_1,
                 table_2,table_columns_2,table_log_2,table_columns_log_2,
                 influx_server,influx_database,influx_user_login,influx_password,influx_port,
                 influx_columns_1,mqtt_topic_1,influx_columns_2,mqtt_topic_2,initial_db):
        
        self.sql_server = sql_server
        self.sql_database = sql_database
        self.sql_user_login = sql_user_login
        self.sql_password = sql_password

        self.table_1 = table_1
        self.table_columns_1 = table_columns_1
        self.table_log_1 = table_log_1
        self.table_columns_log_1 = table_columns_log_1

        self.table_2 = table_2
        self.table_columns_2 = table_columns_2
        self.table_log_2 = table_log_2
        self.table_columns_log_2 = table_columns_log_2


        self.influx_server = influx_server
        self.influx_database = influx_database
        self.influx_user_login = influx_user_login
        self.influx_password = influx_password
        self.influx_port = influx_port
        self.influx_columns_1 = influx_columns_1
        self.mqtt_topic_1 = mqtt_topic_1
        self.influx_columns_2 = influx_columns_2
        self.mqtt_topic_2 = mqtt_topic_2
        self.initial_db = initial_db

        self.df_insert = None
        self.df_influx = None
        self.df_sql = None

        self.df_insert2 = None
        self.df_influx2 = None
        self.df_sql2 = None

        self.df_defect = None

    def stamp_time(self):
        now = datetime.datetime.now()
        print("\nHi this is job run at -- %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))

    def error_msg(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"process":process,"message":msg,"error":e}

        try:
            self.log_to_db(result)
            sys.exit()
        except Exception as e:
            self.info_msg(self.error_msg.__name__,e)
            sys.exit()

    def error_msg2(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"process":process,"message":msg,"error":e}

        try:
            self.log_to_db2(result)
            sys.exit()
        except Exception as e:
            self.info_msg2(self.error_msg2.__name__,e)
            sys.exit()

    def info_msg(self,process,msg):
        result = {"status":constant.STATUS_INFO,"process":process,"message":msg,"error":"-"}
        print(result)

    def info_msg2(self,process,msg):
        result = {"status":constant.STATUS_INFO,"process":process,"message":msg,"error":"-"}
        print(result)

    def ok_msg(self,process):
        result = {"status":constant.STATUS_OK,"process":process,"message":"program running done","error":"-"}
        try:
            self.log_to_db(result)
            print(result)
        except Exception as e:
            self.error_msg(self.ok_msg.__name__,'cannot ok msg to log',e)

    def ok_msg2(self,process):
        result = {"status":constant.STATUS_OK,"process":process,"message":"program running done","error":"-"}
        try:
            self.log_to_db2(result)
            print(result)
        except Exception as e:
            self.error_msg2(self.ok_msg2.__name__,'cannot ok msg to log',e)

    def conn_sql(self):
        try:
            cnxn = pymssql.connect(self.sql_server, self.sql_user_login, self.sql_password, self.sql_database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.info_msg(self.conn_sql.__name__,e)
            sys.exit()

    def conn_sql2(self):
        try:
            cnxn = pymssql.connect(self.sql_server, self.sql_user_login, self.sql_password, self.sql_database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.info_msg2(self.conn_sql2.__name__,e)
            sys.exit()

    def log_to_db(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            cursor.execute(f"""
                INSERT INTO [{self.sql_database}].[dbo].[{self.table_log_1}] 
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
            # self.alert_line("Danger! cannot insert log table")
            self.info_msg(self.log_to_db.__name__,e)
            sys.exit()

    def log_to_db2(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql2()
        try:
            cursor.execute(f"""
                INSERT INTO [{self.sql_database}].[dbo].[{self.table_log_2}] 
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
            # self.alert_line("Danger! cannot insert log table")
            self.info_msg2(self.log_to_db2.__name__,e)
            sys.exit()

class DATA(PREPARE):
    def __init__(self,sql_server,sql_database,sql_user_login,sql_password,
                 table_1,table_columns_1,table_log_1,table_columns_log_1,
                 table_2,table_columns_2,table_log_2,table_columns_log_2,
                 influx_server,influx_database,influx_user_login,influx_password,influx_port,
                 influx_columns_1,mqtt_topic_1,influx_columns_2,mqtt_topic_2,initial_db):
        super().__init__(sql_server,sql_database,sql_user_login,sql_password,
                         table_1,table_columns_1,table_log_1,table_columns_log_1,
                         table_2,table_columns_2,table_log_2,table_columns_log_2,
                         influx_server,influx_database,influx_user_login,influx_password,influx_port,
                         influx_columns_1,mqtt_topic_1,influx_columns_2,mqtt_topic_2,initial_db)      
    
    def query_influx(self) :
        try:
            result_lists = []
            now = datetime.datetime.now()
            current_time_epoch = int(time.time()) * 1000 *1000 *1000
            one_hour_ago = now - datetime.timedelta(hours=1)
            previous_time_epoch = int(one_hour_ago.timestamp()) * 1000 *1000 *1000
            client = InfluxDBClient(host=self.influx_server,port=self.influx_port,username=self.influx_user_login,password=self.influx_password,database=self.influx_database)
            mqtt_topic_value = list(str(self.mqtt_topic_1).split(","))
            for i in range(len(mqtt_topic_value)):
                query = f"select {self.influx_columns_1} from mqtt_consumer where topic = '{mqtt_topic_value[i]}' and time >={previous_time_epoch} and time < {current_time_epoch} order by time desc"
                result = client.query(query)
                result_df = pd.DataFrame(result.get_points())
                result_lists.append(result_df)
            query_influx = pd.concat(result_lists, ignore_index=True)
            
            df = query_influx.copy()
            df_split = df['topic'].str.split('/', expand=True)
            df['process'] = df_split[2]
            df['mc_no'] = df_split[3]
            df["time"] =   pd.to_datetime(df["time"]).dt.tz_convert(None)
            df["time"] = df["time"] + pd.DateOffset(hours=7)    
            df["time"] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')[:-3])
            df.rename(columns={'time':'registered_at'},inplace=True)
            df['d_str1'] = "-"
            df['d_str2'] = "-"
            df = df.fillna(0)
            if not df.empty :
                self.df_influx = df
            else:
                self.df_influx = None
                self.info_msg(self.query_influx.__name__,"influxdb data is emply")
        except Exception as e:
            self.error_msg(self.query_influx.__name__,"cannot query influxdb",e)

    def query_influx2(self) :
        try:
            client = InfluxDBClient(self.influx_server, self.influx_port, self.influx_user_login,self.influx_password, self.influx_database)
            mqtt_topic_value = list(str(self.mqtt_topic_2).split(","))

            now = datetime.datetime.now()
            current_time_epoch = int(time.time()) * 1000 *1000 *1000
            one_hour_ago = now - datetime.timedelta(hours=1)
            previous_time_epoch = int(one_hour_ago.timestamp()) * 1000 *1000 *1000
   
            for i in range(len(mqtt_topic_value)):
                query = f"select {self.influx_columns_2} from mqtt_consumer where topic = '{mqtt_topic_value[i]}' and time >= {previous_time_epoch} and time < {current_time_epoch} " 
                result = client.query(query)
                df_result = pd.DataFrame(result.get_points())
                if not df_result.empty:
                    df_result = df_result.sort_values(by='time',ascending=False)
                    df_result = df_result.fillna(0)
                    df_result = df_result[df_result['topic'] != '0']
                    df_result['judge'] = df_result.groupby('topic')['dmc_ok'].diff().fillna(0) > 0
                    df1 = df_result.groupby('topic').head(1)
                    df2 = df_result[df_result['judge']]

                    for model_value in df1['topic'].unique():
                        df1_filtered = df1[df1['topic'] == model_value]
                        df2_filtered = df2[df2['topic'] == model_value]
                        if not df2_filtered.empty:
                            df = pd.concat([df1_filtered,df2_filtered]).reset_index(drop=True)
                        else:
                            df = df1_filtered
                    df.at[0, 'dmc_ok'] = df['dmc_ok'].sum()
                    df.drop(columns=['judge'],inplace=True)
                    df = df.head(1)        
                    df_split = df['topic'].str.split('/', expand=True)
                    df['process'] = df_split[2]
                    df['mc_no'] = df_split[3]
                    df["time"] =   pd.to_datetime(df["time"]).dt.tz_convert(None)
                    df["time"] = df["time"] + pd.DateOffset(hours=7)    
                    df["time"] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')[:-3])
                    df.rename(columns={'time':'registered_at'},inplace=True)
                    df['d_str1'] = "-"
                    df['d_str2'] = "-"
                    df = df.fillna(0)
                    self.df_influx2 = df
                else:
                    self.df_influx2 = None
                    self.info_msg2(self.query_influx2.__name__,"influxdb2 data is emply")
        except Exception as e:
            self.error_msg2(self.query_influx2.__name__,"cannot query influxdb",e)

    def convert_defect(self):
        try:
            encoded_password = urllib.parse.quote_plus(self.sql_password)
            engine = create_engine(f'mssql+pymssql://{self.sql_user_login}:{encoded_password}@{self.sql_server}/{self.sql_database}')
            sql_query = f"""SELECT * FROM [{self.sql_database}].[dbo].[MASTER_NG]"""
            df_defect = pd.read_sql(sql_query, engine)
            columns = df_defect.columns.tolist()
            self.df_defect = df_defect[columns]

            self.df_influx['ng'] = self.df_influx['data'].map(self.df_defect.set_index('code')['ng'])
            self.df_influx.drop(columns=['data'],inplace=True)
            self.df_influx.rename(columns = {'ng':'data'}, inplace = True) 
            
        except Exception as e:
                self.error_msg(self.convert_defect.__name__,"cannot select with sql code",e)

    def query_sql(self):
        try:
            encoded_password = urllib.parse.quote_plus(self.sql_password)
            engine = create_engine(f'mssql+pymssql://{self.sql_user_login}:{encoded_password}@{self.sql_server}/{self.sql_database}')
            sql_query = f"""SELECT TOP 50 * FROM [{self.sql_database}].[dbo].[{self.table_1}] ORDER by registered_at"""
            df_sql = pd.read_sql(sql_query, engine)
            columns = df_sql.columns.tolist()
            self.df_sql = df_sql[columns]
            if self.df_sql.empty :
                self.info_msg(self.query_sql.__name__,f"data is emply")
            return self.df_sql
            
        except Exception as e:
                self.error_msg(self.query_sql.__name__,"cannot select with sql code",e)

    def query_sql2(self):
        try:
            encoded_password = urllib.parse.quote_plus(self.sql_password)
            engine = create_engine(f'mssql+pymssql://{self.sql_user_login}:{encoded_password}@{self.sql_server}/{self.sql_database}')
            sql_query = f"""SELECT TOP 100 * FROM [{self.sql_database}].[dbo].[{self.table_2}] ORDER by registered_at"""
            df_sql = pd.read_sql(sql_query, engine)
            columns = df_sql.columns.tolist()
            self.df_sql2 = df_sql[columns]
            if self.df_sql.empty :
                self.info_msg2(self.query_sql2.__name__,f"data is emply")
            return self.df_sql2
            
        except Exception as e:
                self.error_msg2(self.query_sql2.__name__,"cannot select with sql code",e)

    def check_duplicate(self):
        try:
            df_from_influx = self.df_influx
            df_from_sql = self.df_sql 
            df_from_sql["registered_at"] = df_from_sql['registered_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')[:-3])
            df_not_duplicate = df_from_influx[~df_from_influx[['registered_at', 'data','mc_no']].apply(tuple, 1).isin(df_from_sql[['registered_at', 'data','mc_no']].apply(tuple, 1))]
            if df_not_duplicate.empty:    
                self.df_insert=None     
                self.info_msg(self.check_duplicate.__name__,f"data is not new for update")
            else:
                self.info_msg(self.check_duplicate.__name__,f"we have data new")
                self.df_insert = df_not_duplicate    
                return constant.STATUS_OK   
        except Exception as e:
            self.error_msg(self.check_duplicate.__name__,"cannot select with sql code",e)

    def check_duplicate2(self):
        try:
            df_from_influx = self.df_influx2
            df_from_sql = self.df_sql2
            df_from_sql["registered_at"] = df_from_sql['registered_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')[:-3])
            df_not_duplicate = df_from_influx[~df_from_influx[['registered_at','mc_no','process']].apply(tuple, 1).isin(df_from_sql[['registered_at', 'data_irssr']].apply(tuple, 1))]
            if df_not_duplicate.empty:    
                self.df_insert2=None     
                self.info_msg2(self.check_duplicate2.__name__,f"data is not new for update")
            else:
                self.info_msg2(self.check_duplicate2.__name__,f"we have data new")
                self.df_insert2 = df_not_duplicate    
                return constant.STATUS_OK   
        except Exception as e:
            self.error_msg2(self.check_duplicate2.__name__,"cannot select with sql code",e)
            
    def df_to_db(self):
        insert_db_value = self.table_columns_1.split(",")
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
                    INSERT INTO [{self.sql_database}].[dbo].[{self.table_1}] 
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

    def df_to_db2(self):
        insert_db_value = self.table_columns_2.split(",")
        col_list = insert_db_value
        cnxn,cursor=self.conn_sql2()
        try:
            if not self.df_insert2 is None:  
                df = self.df_insert2
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
                    INSERT INTO [{self.sql_database}].[dbo].[{self.table_2}] 
                    values(
                        {value}
                        )
                        """
                    cursor.execute(insert_string)
                    cnxn.commit()
                cursor.close()
                self.df_insert2 = None

                self.info_msg2(self.df_to_db2.__name__,f"insert data successfully")     
        except Exception as e:
            print('error: '+str(e))
            self.error_msg2(self.df_to_db2.__name__,"cannot insert df to sql",e)

    def run(self):
        # time.sleep(10)
        self.stamp_time()
        if self.initial_db == 'True':
            self.query_influx()
            self.convert_defect()
            if self.df_influx is not None:
                self.query_sql()
                if self.df_sql.empty:
                    self.df_insert = self.df_influx
                    self.df_to_db()
                    self.ok_msg(self.df_to_db.__name__)
                else:
                    self.check_duplicate()
                    if self.df_insert is not None:
                        self.df_to_db()
                        self.ok_msg(self.df_to_db.__name__)
        else:
            print("db is not initial yet")

    def run2(self):
        # time.sleep(10)
        self.stamp_time()
        if self.initial_db == 'True':
            self.query_influx2()
            if self.df_influx2 is not None:
                self.df_insert2 = self.df_influx2
                self.df_to_db2()
                self.ok_msg2(self.df_to_db2.__name__)

        else:
            print("db is not initial yet")
if __name__ == "__main__":
    print("must be run with main")