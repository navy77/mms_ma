import streamlit as st
import dotenv
import os
import time 
import pymssql
import utils.alert as alert
import json
import pandas as pd
import subprocess

from influxdb import InfluxDBClient
from stlib import mqtt
from utils.crontab_config import crontab_delete,crontab_every_minute,crontab_every_hr,crontab_read,crontab_every_5minute
import plotly.graph_objects as go
import plotly.express as px

def conn_sql(st,server,user_login,password,database):
        try:
            cnxn = pymssql.connect(server,user_login,password,database)
            st.success('SQLSERVER CONNECTED!', icon="✅")
            cnxn.close()
        except Exception as e:
            st.error('Error,Cannot connect sql server :'+str(e), icon="❌")

def create_table(st,server,user_login,password,database,table,table_columns):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+table+''' (
                '''+table_columns+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            st.success('CREATE TABLE SUCCESSFULLY!', icon="✅")
            return True
        except Exception as e:
            if 'There is already an object named' in str(e):
                st.error('TABLE is already an object named ', icon="❌")
            elif 'Column, parameter, or variable' in str(e):
                st.error('define columns mistake', icon="❌")
            else:
                st.error('Error'+str(e), icon="❌")
            return False

def drop_table(st,server,user_login,password,database,table):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor()
        # create table
        try:
            cursor.execute(f'''DROP TABLE {table}''')
            cnxn.commit()
            cursor.close()
            st.success('DROP TABLE SUCCESSFULLY!', icon="✅")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def preview_sqlserver(st,server,user_login,password,database,table,mc_no,process):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(20) * FROM {table} where mc_no = '{mc_no}' and process = '{process}' order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=1500)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def preview_influx(st,influx_server,influx_user_login,influx_password,influx_database,influx_port,column_names,mqtt_topic) :
      try:
            result_lists = []
            client = InfluxDBClient(influx_server, influx_port,influx_user_login,influx_password,influx_database)

            if mqtt_topic.split('/')[0] =='data':
                query1 = f"select time,topic,model,d_str1,d_str2,{column_names} from mqtt_consumer where topic = '{mqtt_topic}' order by time desc limit 5"

                result1 = client.query(query1)
                if list(result1):
                    query_list1 = list(result1)[0]
                    df = pd.DataFrame(query_list1)
                    df.time = pd.to_datetime(df.time).dt.tz_convert('Asia/Bangkok')
                    st.dataframe(df,width=1500)
                else:
                    st.error('Error: influx no data', icon="❌")
            else:
                query1 = f"select time,topic,model,d_str1,d_str2,{column_names} from mqtt_consumer where topic = '{mqtt_topic}' order by time desc limit 5"
                result2 = client.query(query1)
                if list(result2):
                    query_list2 = list(result2)[0]
                    df = pd.DataFrame(query_list2)
                    df.time = pd.to_datetime(df.time).dt.tz_convert('Asia/Bangkok')
                    st.dataframe(df,width=1500)
                else:
                    st.error('Error: influx no data', icon="❌")       

      except Exception as e:
          st.error('Error: '+str(e), icon="❌")

def log_sqlserver(st,server,user_login,password,database,table):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(20) * FROM {table} order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=2000)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def config_project():

    st.header("PROJECT")
    div_name = str(os.environ["DIV"])
    process_name = str(os.environ["PROCESS"])

    table_name_1 = str(os.environ["TABLE_1"])
    table_log_name_1 = str(os.environ["TABLE_LOG_1"])
    table_name_2 = str(os.environ["TABLE_2"])
    table_log_name_2 = str(os.environ["TABLE_LOG_2"])

    init_db = str(os.environ["SQL_INITIAL_DB"])

    with st.form("config_project"):

        col1,col2 = st.columns(2)

        with col1:
            div_name_input = st.text_input('Division Name', div_name,key="div_name_input")
            process_name_input = st.text_input('Process Name', process_name,key="process_name_input")
            defect_data= st.checkbox('DATA DEFECT')
            output_data= st.checkbox('DATA OUTPUT')
            submitted = st.form_submit_button("INITIAL")
               
        if submitted:
            if defect_data:
                os.environ["DIV"] = str(div_name_input.upper())
                os.environ["PROCESS"] = str(process_name.upper())
                os.environ["TABLE_1"] = "DATA_DEFECT"
                os.environ["TABLE_LOG_1"] = "DATA_LOG_1"
                os.environ["PROJECT_TYPE_1"] = "DATA_DEFECT"
            else:
                os.environ["PROJECT_TYPE_1"] = ""
            if output_data:
                os.environ["DIV"] = str(div_name_input.upper())
                os.environ["PROCESS"] = str(process_name_input.upper())
                os.environ["TABLE_2"] = "DATA_OUTPUT"
                os.environ["TABLE_LOG_2"] = "DATA_LOG_2"
                os.environ["PROJECT_TYPE_2"] = "DATA_OUTPUT"
 
            else:
                os.environ["PROJECT_TYPE_2"] = ""

            os.environ["INIT_PROJECT"] = "True"

            dotenv.set_key(dotenv_file,"DIV",os.environ["DIV"])
            dotenv.set_key(dotenv_file,"PROCESS",os.environ["PROCESS"])

            dotenv.set_key(dotenv_file,"TABLE_1",os.environ["TABLE_1"])
            dotenv.set_key(dotenv_file,"TABLE_LOG_1",os.environ["TABLE_LOG_1"])
            dotenv.set_key(dotenv_file,"TABLE_2",os.environ["TABLE_2"])
            dotenv.set_key(dotenv_file,"TABLE_LOG_2",os.environ["TABLE_LOG_2"])

            dotenv.set_key(dotenv_file,"INIT_PROJECT",os.environ["INIT_PROJECT"])
            dotenv.set_key(dotenv_file,"PROJECT_TYPE_1",os.environ["PROJECT_TYPE_1"])
            dotenv.set_key(dotenv_file,"PROJECT_TYPE_2",os.environ["PROJECT_TYPE_2"])
    
            st.rerun()
    
        with col2:
            st.text("PREVIEW ")
            if defect_data:
                st.text("TABLE NAME: "+table_name_1)
                st.text("TABLE LOG NAME: "+table_log_name_1)
            if output_data:
                st.text("TABLE NAME: "+table_name_2)
                st.text("TABLE LOG NAME: "+table_log_name_2)

    st.markdown("---")

def config_mqtt_add():

    st.header("MQTT TOPIC REGISTRY")

    with st.form("config_mqtt_add"):
        div_name =  os.environ["DIV"]
        process_name =  os.environ["PROCESS"]
        project_type_1 = os.environ["PROJECT_TYPE_1"]
        project_type_2 = os.environ["PROJECT_TYPE_2"]

        mqtt_value = None
    
        mqtt_registry = list(str(os.environ["MQTT_TOPIC"]).split(","))
        col1,col2 = st.columns(2)
        
        with col1:
            add_new_mqtt = st.text_input("Add a new mqtt (topic: machine_no)","",key="add_new_mqtt_input")
            add_new_mqtt_but = st.form_submit_button("Add MQTT", type="secondary")
            add_new_mqtt_ = []
            if add_new_mqtt and add_new_mqtt_but:
                if project_type_1!="":
                    add_new_mqtt1 = "data/"+div_name.lower()+"/"+process_name.lower()+"/"+add_new_mqtt.lower()
                else: add_new_mqtt1 = None
                if project_type_2!="":
                    add_new_mqtt2 = "got/"+div_name.lower()+"/"+process_name.lower()+"/"+add_new_mqtt.lower()
                else: add_new_mqtt2 = None

                add_new_mqtt_ = [add_new_mqtt1,add_new_mqtt2]
                if add_new_mqtt1 is None:
                    add_new_mqtt_.remove(add_new_mqtt1)
                if add_new_mqtt2 is None:
                    add_new_mqtt_.remove(add_new_mqtt2)

                mqtt_registry += add_new_mqtt_

                for i in range(len(mqtt_registry)):
                    if mqtt_value == None:
                        mqtt_value = mqtt_registry[i]
                    else:
                        mqtt_value = str(mqtt_value)+","+mqtt_registry[i]

                os.environ["MQTT_TOPIC"] = mqtt_value
                dotenv.set_key(dotenv_file,"MQTT_TOPIC",os.environ["MQTT_TOPIC"])   

                mqtt_1 = None
                mqtt_2 = None
                mqtt_list = mqtt_value.split(",")

                for i in range(len(mqtt_list)):
                    if mqtt_list[i].split('/')[0] == "data":
                        if mqtt_1==None:
                            mqtt_1 = mqtt_list[i]
                        else:
                            mqtt_1 = str(mqtt_1)+","+mqtt_list[i]
                    elif mqtt_list[i].split('/')[0] == "got":
                        if mqtt_2==None:
                            mqtt_2 = mqtt_list[i]
                        else:
                            mqtt_2 = str(mqtt_2)+","+mqtt_list[i]

                os.environ["MQTT_TOPIC_1"] = mqtt_1 
                dotenv.set_key(dotenv_file,"MQTT_TOPIC_1",os.environ["MQTT_TOPIC_1"])
                os.environ["MQTT_TOPIC_2"] = mqtt_2
                dotenv.set_key(dotenv_file,"MQTT_TOPIC_2",os.environ["MQTT_TOPIC_2"])

                st.success('Done!', icon="✅")

                time.sleep(0.5)
                st.rerun()

        with col2:
            st.text("PREVIEW ")
            st.text("MQTT TOPIC REGISTRY: "+str(os.environ["MQTT_TOPIC_1"]))
            st.text("MQTT TOPIC REGISTRY: "+str(os.environ["MQTT_TOPIC_2"])) 

def config_mqtt_delete():

    with st.form("config_mqtt_delete"):

        mqtt_value = None
        mqtt_registry = list(str(os.environ["MQTT_TOPIC"]).split(","))

        col1, col2 = st.columns(2)

        with col1:
    
            option_mqtt = st.multiselect(
                        'Delete mqtt',
                        mqtt_registry,placeholder="select mqtt...")

            delete_mqtt = st.form_submit_button("Delete MQTT", type="primary")

            if delete_mqtt:
                len_mqtt_registry = len(mqtt_registry)
                len_option_mqtt = len(option_mqtt)
                if len_option_mqtt<len_mqtt_registry:
                    
                    for i in range(len(option_mqtt)):
                        mqtt_registry.remove(option_mqtt[i])

                    for i in range(len(mqtt_registry)):
                        if mqtt_value == None:
                            mqtt_value = mqtt_registry[i]
                        else:
                            mqtt_value = str(mqtt_value)+","+mqtt_registry[i]

                    os.environ["MQTT_TOPIC"] = mqtt_value
                    dotenv.set_key(dotenv_file,"MQTT_TOPIC",os.environ["MQTT_TOPIC"])

                    topics_all = mqtt_value.split(',')
                    data_list = []
                    output_list = []
                    for topic in topics_all:
                        if 'data' in topic:
                            data_list.append(topic)
                        elif 'got' in topic:
                            output_list.append(topic)
           

                    os.environ["MQTT_TOPIC_1"] = ','.join(data_list)
                    os.environ["MQTT_TOPIC_2"] = ','.join(output_list)

                    dotenv.set_key(dotenv_file,"MQTT_TOPIC_1",os.environ["MQTT_TOPIC_1"])
                    dotenv.set_key(dotenv_file,"MQTT_TOPIC_2",os.environ["MQTT_TOPIC_2"])

                    st.success('Deleted!', icon="✅")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error('Cannot delete,sensor regisry must have at least one!', icon="❌")

    st.markdown("---")

def config_sensor_registry_add():

    st.header("SENSOR REGISTRY")

    with st.form("config_sensor_registry_add"):

        sensor_value = None
        sensor_registry = list(str(os.environ["COLUMN_NAMES"]).split(","))
        table_column_value = "registered_at datetime,mc_no varchar(10),process varchar(10),model varchar(25),spec varchar(25),d_str1 varchar(15),d_str2 varchar(15)"

        col1,col2 = st.columns(2)
        
        with col1:
            add_new_sensor = st.text_input("Add a sensor address","",key="add_new_sensor_input")
            add_new_sensor_but = st.form_submit_button("Add SENSOR", type="secondary")

            if add_new_sensor and add_new_sensor_but:
                sensor_registry.append(add_new_sensor)
                for i in range(len(sensor_registry)):
                    if sensor_value == None:
                        sensor_value = sensor_registry[i]
                    else:
                        sensor_value = str(sensor_value)+","+sensor_registry[i]
                    table_column_value = table_column_value+","+sensor_registry[i] + " float"

                os.environ["PRODUCTION_TABLE_COLUMNS"] = table_column_value
                os.environ["COLUMN_NAMES"] = sensor_value

                dotenv.set_key(dotenv_file,"COLUMN_NAMES",os.environ["COLUMN_NAMES"])
                dotenv.set_key(dotenv_file,"PRODUCTION_TABLE_COLUMNS",os.environ["PRODUCTION_TABLE_COLUMNS"])            

                st.success('Done!', icon="✅")
                time.sleep(0.5)
                st.rerun()

        with col2:
            st.text("PREVIEW ")
            st.text("SENSOR REGISTRY: "+str(os.environ["COLUMN_NAMES"]))
            st.text("DATATYPE: "+str(os.environ["PRODUCTION_TABLE_COLUMNS"]))

def config_sensor_registry_delete():

    with st.form("config_sensor_registry_delete"):

        sensor_value = None
        sensor_registry = list(str(os.environ["COLUMN_NAMES"]).split(","))
        table_column_value = "registered_at datetime,mc_no varchar(10),process varchar(10),model varchar(25),spec varchar(25),d_str1 varchar(15),d_str2 varchar(15)"

        col1, col2 = st.columns(2)

        with col1:
    
            option_sensor = st.multiselect(
                        'Delete sensor',
                        sensor_registry,placeholder="select sensor...")

            delete_sensor = st.form_submit_button("Delete SENSOR", type="primary")

            if delete_sensor:
                len_sensor_registry = len(sensor_registry)
                len_option_sensor = len(option_sensor)
                if len_option_sensor<len_sensor_registry:
                    
                    for i in range(len(option_sensor)):
                        sensor_registry.remove(option_sensor[i])

                    for i in range(len(sensor_registry)):
                        if sensor_value == None:
                            sensor_value = sensor_registry[i]
                        else:
                            sensor_value = str(sensor_value)+","+sensor_registry[i]
                        table_column_value = table_column_value+","+sensor_registry[i] + " float"

                    os.environ["PRODUCTION_TABLE_COLUMNS"] = table_column_value
                    os.environ["COLUMN_NAMES"] = sensor_value
                    dotenv.set_key(dotenv_file,"COLUMN_NAMES",os.environ["COLUMN_NAMES"])
                    dotenv.set_key(dotenv_file,"PRODUCTION_TABLE_COLUMNS",os.environ["PRODUCTION_TABLE_COLUMNS"])

                    st.success('Deleted!', icon="✅")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error('Cannot delete,sensor regisry must have at least one!', icon="❌")

def config_db_connect(env_headers):
    if env_headers == "SQLSERVER":
        form_name = "config_db_connect_sql"
    elif env_headers == "INFLUXDB":
        form_name = "config_db_connect_influx"

    with st.form(form_name):

        total_env_list = None
        if env_headers == "SQLSERVER":
            total_env_list = sql_server_env_lists = ["SQL_SERVER","SQL_DATABASE","SQL_USER_LOGIN","SQL_PASSWORD"]
        elif env_headers == "INFLUXDB":
            total_env_list = influxdb_env_lists = ["INFLUX_SERVER","INFLUX_DATABASE","INFLUX_USER_LOGIN","INFLUX_PASSWORD","INFLUX_PORT"]
        else :
            st.error("don't have the connection")

        if total_env_list is not None:
            st.header(env_headers)
            cols = st.columns(len(total_env_list))
            for j in range(len(total_env_list)):
                param = total_env_list[j]
                if "PASSWORD" in param or "TOKEN" in param:
                    type_value = "password"
                else:
                    type_value = "default"
                os.environ[param] = cols[j].text_input(param,os.environ[param],type=type_value)
                dotenv.set_key(dotenv_file,param,os.environ[param])

            cols = st.columns(2) 

            if env_headers == "SQLSERVER":

                sql_check_but = cols[0].form_submit_button("CONECTION CHECK")
                if sql_check_but:
                    conn_sql(st,os.environ["SQL_SERVER"],os.environ["SQL_USER_LOGIN"],os.environ["SQL_PASSWORD"],os.environ["SQL_DATABASE"])

            elif env_headers == "INFLUXDB":
                influx_check_but = cols[0].form_submit_button("CONECTION CHECK")
                if influx_check_but:
                    try:
                        client = InfluxDBClient(os.environ["INFLUX_SERVER"], os.environ["INFLUX_PORT"], os.environ["INFLUX_USER_LOGIN"], os.environ["INFLUX_PASSWORD"], os.environ["INFLUX_DATABASE"])
                        # result = client.query('select * from mqtt_consumer order by time limit 1')
                        client.ping()
                        st.success('INFLUXDB CONNECTED!', icon="✅")
                    except Exception as e:
                        st.error("Error :"+str(e))
            else:
                st.error('Dont have the connection!', icon="❌")

    st.markdown("---")

def config_initdb():
        st.header("DB STATUS")
        initial_db_value = os.environ["SQL_INITIAL_DB"]
        if initial_db_value == "False":
            st.error('DB NOT INITIAL', icon="❌")
            st.write("PLEASE CONFIRM CONFIG SETUP BEFORE INITIAL")
            initial_but = st.button("INITIAL")
            if initial_but:
                if os.environ["PROJECT_TYPE_1"] == "PRODUCTION":
                    table_column_1 = "PRODUCTION_TABLE_COLUMNS"
                else: table_column_1 = None

                if os.environ["PROJECT_TYPE_2"] == "MCSTATUS":
                    table_column_2 = "MCSTATUS_TABLE_COLUMNS"
                else: table_column_2 = None

                if os.environ["PROJECT_TYPE_3"] == "ALARMLIST":
                    table_column_3 = "ALARMLIST_TABLE_COLUMNS"
                else:table_column_3 = None
  
                if table_column_1 is not None:
                    result_1 = create_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_1"],os.environ[table_column_1])
                    result_2 = create_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_1"],os.environ["TABLE_COLUMNS_LOG"])

                if table_column_2 is not None:
                    result_3 = create_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_2"],os.environ[table_column_2])
                    result_4 = create_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_2"],os.environ["TABLE_COLUMNS_LOG"])

                if table_column_3 is not None:
                    result_5 = create_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_3"],os.environ[table_column_3])
                    result_6 = create_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_3"],os.environ["TABLE_COLUMNS_LOG"])


                if result_1 and result_2 and result_3 and result_4 and result_5 and result_6 is not False:
                    os.environ["INITIAL_DB"] = "True"
                    dotenv.set_key(dotenv_file,"INITIAL_DB",os.environ["INITIAL_DB"])
                    dotenv.set_key(dotenv_file,"PRODUCTION_TABLE_COLUMNS",os.environ["PRODUCTION_TABLE_COLUMNS"])     
                    st.rerun()
                # else:
                #     st.error('UNKNOWN PROJECT TYPE', icon="❌")
        else:
            st.success('DB CREATED!', icon="✅")
            with st.expander("DELETE DB"):
                st.error('DANGER ZONE!!!!! PLEASE BACKUP DB BEFORE REMOVE')
                st.write("DELETE TABLE:  "+os.environ["TABLE_1"])
                st.write("DELETE TABLE:  "+os.environ["TABLE_LOG_1"])
                st.write("DELETE TABLE:  "+os.environ["TABLE_2"])
                st.write("DELETE TABLE:  "+os.environ["TABLE_LOG_2"])
                st.write("DELETE TABLE:  "+os.environ["TABLE_3"])
                st.write("DELETE TABLE:  "+os.environ["TABLE_LOG_3"])

                remove_input = st.text_input("PASSWORD","",type="password")
                remove_but = st.button("REMOVE DB")
                if remove_but:
                    if remove_input=="mic@admin":
                        drop_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_1"])
                        drop_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_1"])
                        drop_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_2"])
                        drop_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_2"])
                        drop_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_3"])
                        drop_table(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_3"])
                        os.environ["INITIAL_DB"] = "False"
                        os.environ["INIT_PROJECT"] = "False"
                        dotenv.set_key(dotenv_file,"INITIAL_DB",os.environ["INITIAL_DB"])
                        dotenv.set_key(dotenv_file,"INIT_PROJECT",os.environ["INIT_PROJECT"])
                        st.success('Deleted!', icon="✅")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error('Cannot delete,password mistake!', icon="❌")

def line_alert():
        st.header("LINE NOTIFY")
        line_notify_flag_value = os.environ["LINE_NOTIFY_FLAG"]

        line_notify_token_input = st.text_input("LINE NOTIFY TOKEN",os.environ["LINE_NOTIFY_TOKEN"],type="password")
 
        if line_notify_token_input:
            alert_toggle = st.toggle('Activate line notify feature',value=eval(line_notify_flag_value))

            if alert_toggle:
                line_notify_flag_value = 'True'
                os.environ["LINE_NOTIFY_FLAG"] = line_notify_flag_value
                st.success('LINE NOTIFY ACTIVED!', icon="✅")
            else:
                line_notify_flag_value = 'False'
                os.environ["LINE_NOTIFY_FLAG"] = line_notify_flag_value
                st.error('LINE NOTIFY DEACTIVED!', icon="❌")

            os.environ["LINE_NOTIFY_TOKEN"] = line_notify_token_input
            dotenv.set_key(dotenv_file,"LINE_NOTIFY_FLAG",os.environ["LINE_NOTIFY_FLAG"])
            dotenv.set_key(dotenv_file,"LINE_NOTIFY_TOKEN",os.environ["LINE_NOTIFY_TOKEN"])
        
            st.markdown("---")

            if alert_toggle:
                st.write("ALERT CONNECTION CHECK")
                cols = st.columns(2) 

                cols[0].caption("LINE NOTIFY CHECK")
                line_check_but = cols[0].button("CHECK",key="line_notify_check")
        
                if line_check_but:
                    status = alert.line_notify(os.environ["LINE_NOTIFY_TOKEN"],"Test send from "+os.environ["TABLE"]+" project")
                    status_object = json.loads(status)
                    if status_object["status"] == 401:
                        st.error("Error: "+status_object["message"], icon="❌")
                    else:
                        st.success('SUCCESSFUL SENDING LINE NOTIFY!', icon="✅")

                st.markdown("---")

def dataflow_production_mqtt():
        st.caption("MQTT")
        mqtt_broker_input = st.text_input('MQTT Broker', os.environ["MQTT_BROKER"])
        os.environ["MQTT_BROKER"] = mqtt_broker_input
        dotenv.set_key(dotenv_file,"MQTT_BROKER",os.environ["MQTT_BROKER"])
        type_data = st.radio("Type of data",["defect","output"],horizontal =True,key="1")
        if type_data=="defect":
            mqtt_registry = list(str(os.environ["MQTT_TOPIC_1"]).split(","))
        elif type_data=="output":
            mqtt_registry = list(str(os.environ["MQTT_TOPIC_2"]).split(","))

        preview_mqtt_selectbox = st.selectbox(
                "mqtt topic",
                mqtt_registry,
                index=None,
                placeholder="select topic...",
                key='preview_mqtt'
                    )

        if preview_mqtt_selectbox:
            cols = st.columns(9)
            preview_mqtt_but = cols[0].button("CONNECT",key="preview_mqtt_but")
            stop_mqtt_but = cols[1].button("STOP",key="stop_mqtt_but",type="primary")
            if preview_mqtt_but:
                mqtt.run_subscribe(st,os.environ["MQTT_BROKER"],1883,preview_mqtt_selectbox)


        st.markdown("---")

def dataflow_production_influx():
        st.caption("INFLUXDB")
        type_data = st.radio("Type of data",["defect","output"],horizontal =True,key="2")
        if type_data=="defect":
            mqtt_registry = list(str(os.environ["MQTT_TOPIC_1"]).split(","))
            influx_column = os.environ["INFLUX_COLUMNS_1"]
        else:
            mqtt_registry = list(str(os.environ["MQTT_TOPIC_2"]).split(","))
            influx_column= os.environ["INFLUX_COLUMNS_2"]
        
        preview_influx_selectbox = st.selectbox(
                "mqtt topic",
                mqtt_registry,
                index=None,
                placeholder="select topic...",
                key='preview_influx'
                    )
        if preview_influx_selectbox:
            preview_influx_but = st.button("QUERY",key="preview_influx_but")
        
            if preview_influx_but:
                preview_influx(st,os.environ["INFLUX_SERVER"],os.environ["INFLUX_USER_LOGIN"],os.environ["INFLUX_PASSWORD"],os.environ["INFLUX_DATABASE"],os.environ["INFLUX_PORT"],influx_column,preview_influx_selectbox)
        st.markdown("---")

def dataflow_test():
        st.caption("TEST RUN DATA")
        test_run_but = st.button("TEST",key="test_run_but")
        if test_run_but:
            try:
                result = subprocess.check_output(['python', 'main.py'])
                st.write(result.decode('UTF-8'))
                st.success('TEST RUN SUCCESS!', icon="✅")
            except Exception as e:
                st.error("Error :"+str(e))
        st.markdown("---")

def dataflow_test2():
        st.caption("TEST RUN THE STATUS/ALARM DATA")
        test_run_but2 = st.button("TEST",key="test_run_but2")
        if test_run_but2:
            try:
                result = subprocess.check_output(['python', 'main2.py'])
                st.write(result.decode('UTF-8'))
                st.success('TEST RUN SUCCESS!', icon="✅")
            except Exception as e:
                st.error("Error :"+str(e))
        st.markdown("---")

def preview_production_sqlserver(server,user_login,password,database,table,mc_no,process):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(5) * FROM {table} where mc_no = '{mc_no}' and process = '{process}' order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=1500)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def getdata_sqlserver(server,user_login,password,database,table):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT * FROM {table} order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                return df
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def dataflow_production_sql():
        st.caption("SQLSERVER")
        type_data = st.radio("Type of data",["defect","output"],horizontal =True,key="3")
        if type_data=="defect":
            mqtt_registry = list(str(os.environ["MQTT_TOPIC_1"]).split(","))
        elif type_data=="output":
            mqtt_registry = list(str(os.environ["MQTT_TOPIC_2"]).split(","))


        preview_sqlserver_selectbox = st.selectbox(
                "mqtt topic",
                mqtt_registry,
                index=None,
                placeholder="select topic...",
                key='preview_sqlserver'
                    )
        if preview_sqlserver_selectbox:
            preview_sqlserver_but = st.button("QUERY",key="preview_sqlserver_but")
        
            if preview_sqlserver_but:
                mc_no = preview_sqlserver_selectbox.split("/")[3]
                process = preview_sqlserver_selectbox.split("/")[2]
                project_type = preview_sqlserver_selectbox.split("/")[0]

                if project_type == "data":
                    table = os.environ["TABLE_1"]
                elif project_type == "got":
                    table = os.environ["TABLE_2"]

                preview_production_sqlserver(os.environ["SQL_SERVER"],os.environ["SQL_USER_LOGIN"],os.environ["SQL_PASSWORD"],os.environ["SQL_DATABASE"],table,mc_no,process)
        st.markdown("---")

def logging():
    st.header("LOG")
    if os.environ["SQL_INITIAL_DB"] == "True":
        log_sqlserver(st,os.environ["SQL_SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG_1"])
    else:
        st.error('DB NOT INITIAL', icon="❌")

def monitor_chart():
    st.write("Monitor Defect")
    column_master_list = ['snap_open', 'snap_crack', 'snap_broken', 'shaft_ng','grease_leak','op_dent','laser_ng','foreign','cover_dent','cover_short','pin_ng','con_ng','other']
    get_chart = st.button("Get chart")
    if get_chart:
        defect_data = getdata_sqlserver(os.environ["SQL_SERVER"],os.environ["SQL_USER_LOGIN"],os.environ["SQL_PASSWORD"],os.environ["SQL_DATABASE"],os.environ["TABLE_1"])
        output_data = getdata_sqlserver(os.environ["SQL_SERVER"],os.environ["SQL_USER_LOGIN"],os.environ["SQL_PASSWORD"],os.environ["SQL_DATABASE"],os.environ["TABLE_2"])

        defect_data['registered_at'] = pd.to_datetime(defect_data['registered_at'])
        output_data['registered_at'] = pd.to_datetime(output_data['registered_at'])

        defect_data['hour'] = defect_data['registered_at'].dt.floor('h')
        output_data['hour'] = output_data['registered_at'].dt.floor('h')
        output_data['registered_at'] = output_data['hour']

        pivot_df1 = defect_data.pivot_table(index=['hour', 'mc_no', 'process'], columns='data', aggfunc='size', fill_value=0).reset_index()
        merged_df = pd.merge(output_data, pivot_df1, left_on=['hour', 'mc_no', 'process'], right_on=['hour', 'mc_no', 'process'], how='left')
        merged_df = merged_df.fillna(0)
        merged_df = merged_df.drop(columns=['hour'])

        df = merged_df.copy()
        for column in column_master_list:
            if column not in df.columns:
                df[column] = 0.0
        print(defect_data)
        print(output_data)
        print(df)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['snap_open'],name='snap_open',yaxis='y2',marker=dict(color='#636EFA'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['snap_crack'],name='snap_crack',yaxis='y2',marker=dict(color='#EF553B'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['snap_broken'],name='snap_broken',yaxis='y2',marker=dict(color='#00CC96'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['shaft_ng'],name='shaft_ng',yaxis='y2',marker=dict(color='#AB63FA'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['grease_leak'],name='grease_leak',yaxis='y2',marker=dict(color='#FFA15A'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['op_dent'],name='op_dent',yaxis='y2',marker=dict(color='#FF6692'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['foreign'],name='foreign',yaxis='y2',marker=dict(color='#B6E880'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['cover_dent'],name='cover_dent',yaxis='y2',marker=dict(color='#FECB52'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['cover_short'],name='cover_short',yaxis='y2',marker=dict(color='#2ca02c'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['pin_ng'],name='pin_ng',yaxis='y2',marker=dict(color='#ff7f0e'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['con_ng'],name='con_ng',yaxis='y2',marker=dict(color='#bcbd22'),opacity=0.3))
        fig.add_trace(go.Bar(x=df['registered_at'],y=df['other'],name='other',yaxis='y2',marker=dict(color='#17becf'),opacity=0.3))


        fig.add_trace(go.Scatter(
            x=df['registered_at'],
            y=df['dmc_ok'],
            mode='lines+markers',
            name='dmc_ok',
            line=dict(color='blue', width=3),
            marker=dict(size=10),
            yaxis='y1'
        ))


        fig.update_layout(
            title=dict(
                text='DMC output and Defect',
                x=0.5,  
                xanchor='center',
                font=dict(size=24)
            ),
            xaxis=dict(title='Time'),
            yaxis=dict(
                title='DMC_OK [pcs]',
                titlefont=dict(color='black'),
                tickfont=dict(color='black'),
                showgrid=False,
            ),
            yaxis2=dict(
                title='Defect [pcs]',
                titlefont=dict(color='black'),
                tickfont=dict(color='black'),
                overlaying='y',
                side='right'
            ),
            barmode='stack',
            legend=dict(
                x=1.05,  
                y=1,     
                orientation="v"  
            ),
            width=1500,
            height=500
        )
        st.plotly_chart(fig)


def main_layout():
    st.set_page_config(
            page_title="MMS Config 1.3.0",
            page_icon="🗃",
            layout="wide",
            initial_sidebar_state="expanded",
        )
    st.markdown("""<h1 style='text-align: center;'>MACHINE MONITORING SYSTEM CONFIG</h1>""", unsafe_allow_html=True)

    text_input_container = st.empty()
    t = text_input_container.text_input("Input password", type="password")

    if t == "1":
        text_input_container.empty()
         
        tab1, tab2 , tab3 ,tab4 , tab5  = st.tabs(["⚙️ PROJECT CONFIG", "🔑 DB CONNECTION", "📝 CHART ", "🔍 DATAFLOW PREVIEW","🕞SCHEDULE"])
        
        with tab1:
            config_project()
            project_type_1 = os.environ["PROJECT_TYPE_1"]

            init_project = os.environ["INIT_PROJECT"]  
            if init_project == "True": 
                config_mqtt_add()
                config_mqtt_delete()

            else:
                st.error('NOT INITIAL A PROJECT YET', icon="❌")

        with tab2:
            config_db_connect("SQLSERVER")
            config_db_connect("INFLUXDB")

        with tab3:
            monitor_chart()
       
        with tab4:
            st.header("DATAFLOW PREVIEW")

            dataflow_production_mqtt()
            dataflow_production_influx()
            dataflow_test()
            dataflow_production_sql()


        with tab5:
            crontab_value = st.selectbox('Select Schedule',('Every 1 minute','Every 5 minute', 'Hourly'))
            crontab_but = st.button("SUBMIT")
            st.markdown("---")
            st.subheader("READ CRONTAB")
            st.markdown("---")
            st.write(crontab_read())
            st.markdown("---")
            if crontab_but:
                    if crontab_value == 'Every 1 minute':
                        crontab_delete()
                        crontab_every_minute()
                        subprocess.call(['sh', './run_crontab.sh'])
                        st.rerun()
                    elif crontab_value == 'Every 5 minute':
                        crontab_delete()
                        crontab_every_5minute()
                        subprocess.call(['sh', './run_crontab.sh'])
                        st.rerun()
                    elif crontab_value == 'Hourly':
                        crontab_delete()
                        crontab_every_hr()
                        subprocess.call(['sh', './run_crontab.sh'])
                        st.rerun()
                    else:
                        st.error("Error: crontab unknown")
    elif t == "":
        pass
    else:
        st.toast('PASSWORD NOT CORRECT!', icon='❌')
            
if __name__ == "__main__":
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)
    main_layout()