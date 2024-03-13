#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import urllib3
import json
import hashlib
urllib3.disable_warnings()
from datetime import datetime, timedelta
import psycopg2
import time
import sys
from psycopg2.extras import execute_values
from copy import deepcopy


# In[2]:


def F_GetAuthToken():
    url = "https://172.20.0.51:28088/v1.1/main/action/itaUserLogin"
#     url = "https://61.75.178.52:28088/v1.1/main/action/itaUserLogin"
    encoded_str = "106da986b88086246844c2c6370972b52902bac25d788e4fa9a6f2cc14cfb25a"
    body = {
        "userId": "one_hour@kwater.or.kr",
        "userPassword": encoded_str
    }
    headers = {'Content-Type': 'application/json', 'X-CSRF-TOKEN': "UL"}

    body = json.dumps(body)
    response = requests.post(url, headers=headers, data=body, verify=False)
    
    if response.status_code == 200:
        data = response.json()
        auth_token = data['authToken']
        print('새로운 토큰을 가져왔습니다:', auth_token)
        return auth_token
    else:
        print('토큰 가져오기 실패:', response.status_code)
        return None


# In[3]:


# 60디비
# def F_ConnectPostDB():
#     host = '172.20.0.60'
#     port = '5432'
#     user = 'postgres'
#     password = 'postgres'
#     database = 'postgres'

# 62디비   
def F_ConnectPostDB():
    host = '172.20.0.62'
    port = '5432'
    user = 'postgres'
    password = 'qntksedc1!'
    database = 'postgres'

# 테스트디비
# def F_ConnectPostDB():
#     host = '127.0.0.1'
#     port = '5432'
#     user = 'postgres'
#     password = 'postgres'
#     database = 'postgres'
    
    global isconnect
    
    try :
        # PostgreSQL 연결
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

        cursor = connection.cursor()
        print("PostgreSQL 데이터베이스 연결 성공!")
        
        isconnect = 1
        
    except (Exception, psycopg2.Error) as error:
        print("PostgreSQL 오류: ",error)

    return connection, cursor


# In[4]:


def F_ConnectionClose(cursor, connection):
    cursor.close()
    connection.close()
    isconnect = 0
    print("데이터 베이스 연결 해제")


# In[5]:


class DataTable:
    def __init__(self, tablename, model_names):
        self.tablename = tablename
        self.model_names = model_names

    #attrs url
    def get_data_from_api_attrs(self, url, auth_token, cur_model_index):
        print("데이터 받아오는 중", cur_model_index)
        
        model_list = self.model_names
        url_thing_data = url + model_list[cur_model_index] + "/attrs"
        params = {
            "limit": '1000',
            #"timeTo": '2024-01-25T00:00:00.000Z',
            #"timeFrom": '2024-01-22T23:59:00.000Z',
            "timeTo": dt_to,
            "timeFrom": dt_from,
            "latestFirst": latestFirst
        }
        get_thing_header = {'Content-Type': 'application/json', 'X-CSRF-TOKEN': "UL " + auth_token}

        cur_response = requests.get(url_thing_data, params=params, headers=get_thing_header, verify=False)

        time.sleep(0.01)  # 때때로 데이터 안들어올 때 있길래 딜레이 줘 봄
        data = cur_response.json()

        if cur_response.status_code != 401:
            # 응답 데이터 출력
#             print("API 응답 데이터:", cur_response.json())
            return cur_response.json()
        else:
            print("중복 로그인 오류 발생")
            return None

        return data

    
    def mainf(self, url, auth_token, cursor, connection):
        
        data_list = []  # 여러 행의 데이터를 저장할 리스트

        for i in range(len(self.model_names)):
            cur_data = self.get_data_from_api_attrs(url, auth_token, i)
            if(len(cur_data['attrs']) == 0):
                print("데이터 없음")
                continue
                
            if cur_data is None:
                return True

            
            for n in range(0, len(cur_data['attrs'][0]['attrValues'])): 
                pre_data_list = self.cleandata(cur_data, n)  # 수정된 부분
                data_list.extend(pre_data_list)

        self.InputDataPostDB(data_list, cursor, connection)
        print("mainf완")
        return False
        


# In[6]:


class HomeNetworkEnergy(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'ENERGY':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            control_type_data = data.get('deviceInfos')
            if control_type_data:
                control_info = json.loads(control_type_data)

                # Assuming 'dongho' info is handled separately
                control_info.pop('dongho', None)

                for device_id, attributes in control_info.items():
                    usage_gas = attributes.get("gas_usage", "N/A")
                    usage_power = attributes.get("electricity_usage", "N/A")
                    usage_heating = attributes.get("heating_usage", "N/A")
                    usage_hotwater = attributes.get("hot_water_usage", "N/A")
                    usage_water = attributes.get("water_usage", "N/A")

                    query = """
                        INSERT INTO home_network_energy (control_time, control_type, thing_type, thing_type_id, usage_gas, 
                                                          usage_power, usage_heating, usage_hotwater, usage_water, phone_number, dong, 
                                                          ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                str(device_id),
                                usage_gas,
                                usage_power,
                                usage_heating,
                                usage_hotwater,
                                usage_water,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                                
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()


# In[7]:


class HomeNetworkLight(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'LIGHT':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 라이트 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    switch_val = attributes.get("switchVal", "N/A")

                    # 데이터베이스에 데이터 삽입
                    query = """
                        INSERT INTO home_network_light (control_time, control_type, thing_type, thing_type_id,
                                                         switch_status, phone_number, dong, ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                switch_val,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()


# In[8]:


class HomeNetworkHeater(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'HEATER':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            control_type_data = data.get('deviceInfos')
            if control_type_data:
                control_info = json.loads(control_type_data)

                # Assuming 'dongho' info is handled separately
                control_info.pop('dongho', None)

                for device_id, attributes in control_info.items():
                    # Extract the required values
                    heating_setpoint_val = attributes.get("heatingSetpointVal", "N/A")
                    thermostat_mode_val = attributes.get("thermostatModeVal", "N/A")
                    temperature_val = attributes.get("temperatureVal", "N/A")

                    

                    # Insert data into the database
                    query = """
                        INSERT INTO home_network_heater (control_time, control_type, thing_type, thing_type_id, heater_id, 
                                                          heater_set_temperature, heater_status, air_temperature, phone_number, dong, 
                                                          ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                json.dumps(data['deviceInfos']),
                                device_id,
                                heating_setpoint_val,
                                thermostat_mode_val,
                                temperature_val,
                                
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                                
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()


# In[9]:


class HomeNetworkAircon(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'AIRCON':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 에어컨 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    switch_val = attributes.get("switchVal", "N/A")
                    cooling_setpoint_val = attributes.get("coolingSetpointVal", "N/A")
                    temperature_val = attributes.get("temperatureVal", "N/A")

                    # Insert data into the database
                    query = """
                        INSERT INTO home_network_aircon (control_time, control_type, thing_type, thing_type_id,
                                                          aircon_status, aircon_set_temperature, air_temperature, phone_number, dong, 
                                                          ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                switch_val,
                                cooling_setpoint_val,
                                temperature_val,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()


# In[10]:


class HomeNetworkCurtain(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'CURTAIN':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 라이트 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    window_shade_val = attributes.get("windowShadeVal", "N/A")

                    # 데이터베이스에 데이터 삽입
                    query = """
                        INSERT INTO home_network_curtain (control_time, control_type, thing_type, thing_type_id,
                                                         thing_status, phone_number, dong, ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                window_shade_val,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()


# In[11]:


class HomeNetworkAirpurifier(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'HOMECUBE':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 라이트 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    pm10 = attributes.get("fineDust", "N/A")
                    pm10_level = attributes.get("fineLevel", "N/A")
                    thing_status = attributes.get("onoff", "N/A")

                    # 데이터베이스에 데이터 삽입
                    query = """
                        INSERT INTO home_network_airpurifier (control_time, control_type, thing_type, thing_type_id, 
                        pm10, pm10_level,thing_status, phone_number, dong, ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                pm10,
                                pm10_level,
                                thing_status,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()
                        


# In[12]:


class HomeNetworkPatternLight(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'PATTERN LIGHT':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 라이트 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    switch_val = attributes.get("switchVal", "N/A")
                    

                    # 데이터베이스에 데이터 삽입
                    query = """
                        INSERT INTO home_network_patternlight (control_time, control_type, thing_type, thing_type_id, 
                        switch_status, phone_number, dong, ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                switch_val,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()
                        


# In[13]:


class HomeNetworkVentilator(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'VENTILATOR':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 라이트 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    switch_val = attributes.get("switchVal", "N/A")
                    fan_speed_val = attributes.get("fanSpeedVal", "0")

                    # 데이터베이스에 데이터 삽입
                    query = """
                        INSERT INTO home_network_ventilator (control_time, control_type, thing_type, thing_type_id, 
                        thing_status, fan_speed, phone_number, dong, ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                switch_val,
                                fan_speed_val,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()
                     


# In[14]:


class HomeNetworkLight(DataTable):
    def __init__(self, tablename, model_names, indicators):
        super().__init__(tablename, model_names)
        self.indicators = indicators

    def cleandata(self, data, n):
        pre_data_dict = {key: [] for key in self.indicators}
        thing_name = data["thingName"]
        copies = data["attrs"]
        
        for copy in copies:
            for key in self.indicators:
                if key in copy['attrKey']:
                    attr_values = copy['attrValues']
                    pre_data_dict[key].append(attr_values[n]['attrValue'])
                    
        pre_data_dict['created_at'].append(copies[0]['attrValues'][n]['createdAt'])
        pre_data_dict['thingName'] = [thing_name] * len(pre_data_dict[self.indicators[0]])      

        # 리스트 컴프리헨션 대신 반복문 사용
        pre_data_dict_filtered = {}
        for key in pre_data_dict:
            pre_data_dict_filtered[key] = []
            for i in range(len(pre_data_dict[key])):
                if pre_data_dict['deviceType'][i] == 'LIGHT':
                    pre_data_dict_filtered[key].append(pre_data_dict[key][i])
        
        pre_data_list = []
        
        for values in zip(*pre_data_dict_filtered.values()):
            pre_data_dict_item = {}
            for key, value in zip(pre_data_dict_filtered.keys(), values):
                pre_data_dict_item[key] = value
            pre_data_list.append(pre_data_dict_item)

        return pre_data_list


    # InputDataPostDB 메서드 수정 부분
    def InputDataPostDB(self, data_list, cursor, connection):
        print("Starting data insertion...")
        for data in data_list:
            device_infos_data = data.get('deviceInfos')
            if device_infos_data:
                device_infos = json.loads(device_infos_data)

                # 'dongho' 정보는 별도로 처리
                dongho_info = device_infos.pop('dongho', None)

                # 각 라이트 장치에 대해 반복
                for device_id, attributes in device_infos.items():
                    switch_val = attributes.get("switchVal", "N/A")

                    # 데이터베이스에 데이터 삽입
                    query = """
                        INSERT INTO home_network_light (control_time, control_type, thing_type, thing_type_id,
                                                         switch_status, phone_number, dong, ho, thing_model)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                                data['created_at'],
                                data['msgCategory'],  
                                data['deviceType'],
                                device_id,
                                switch_val,
                                data['crtr_id'],  
                                data['dngno'],  
                                data['hono'],  
                                data['ihd_id']
                            )
                    try:
                        cursor.execute(query, values)
                        connection.commit()
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()


# In[15]:


now = datetime.utcnow()
one_minute_ago = now - timedelta(minutes=1)
# datetime 객체를 원하는 형식으로 문자열로 변환합니다.
now_str = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
one_minute_ago_str = one_minute_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
print(one_minute_ago_str, type(one_minute_ago_str))


# In[16]:


##############################전역변수##################################
#15분 단위시간
delta_time = timedelta(minutes=15) 
isconnect = 0
error = 0

#데이터 추출 시간 정의(현재시각 - 15분)
dt = datetime.utcnow()- delta_time


#BIOT 파라미터 모음   <<<<<<<<<<<<<<<<<<< 수정 가능.
dt_from = None
# dt_from = dt.isoformat(timespec='milliseconds')+'Z'   #현재 UTC시간 - 15분 
dt_to = None
limit = "80"
latestFirst = None

#url
url = "https://172.20.0.51:28088/v1.0/sites/C000000003/things/"
# url = "https://61.75.178.52:28088/v1.0/sites/C000000003/things/"

    
tbl_home_network_energy = HomeNetworkEnergy('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)
tbl_home_network_light = HomeNetworkLight('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)

tbl_home_network_heater = HomeNetworkHeater('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)
tbl_home_network_aircon = HomeNetworkAircon('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']
)                                            
tbl_home_network_curtain = HomeNetworkCurtain('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)                                              
tbl_home_network_airpurifier = HomeNetworkAirpurifier('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)                                                      
tbl_home_network_patternlight = HomeNetworkPatternLight('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)                                                        
tbl_home_network_ventilator = HomeNetworkVentilator('home_network_light',
    ['HOMENET_DATA_SVR.prd_cl-CHN_P_Batch'],  
    ['ihd_id', 'crtr_id', 'deviceType', 'msgCategory', 
     'deviceInfos', 'dngno', 'hono','created_at']  
)                                                    


# In[17]:


def job():
    global error
    print("시작: ",datetime.now())
    isconnect = 0

    #authtoken 생성
    auth_token = F_GetAuthToken()
    connection, cursor = None, None

    #DB연결  
    if (isconnect == 0):
        connection, cursor = F_ConnectPostDB()
        time.sleep(1)

    #클래스 별 기능 수행 함수
    function_list = [
                    tbl_home_network_energy.mainf,
                    tbl_home_network_light.mainf,
                    tbl_home_network_heater.mainf,
                    tbl_home_network_aircon.mainf,
                    tbl_home_network_curtain.mainf,
                    tbl_home_network_airpurifier.mainf,
                    tbl_home_network_patternlight.mainf,
                    tbl_home_network_ventilator.mainf
                    
    ]

    for func in function_list:
        result = func(url, auth_token, cursor, connection)
        print(func, " 작업 완료")
        if result == True:
            print("오류, 처음부터 다시 시작")
            return True
        connection.commit()
        print("저장완료")

    #입력 해제
    F_ConnectionClose(cursor, connection)

    print("모든 작업 완료. 3초 후 자동으로 꺼집니다.")
    time.sleep(3)

    return False


# In[18]:


# schedule.every(2).minutes.do(job)
while True:
    error = job()
    if(error == False):
        break
        


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




