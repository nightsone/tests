#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import pandas as pd
from datetime import datetime
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# In[2]:


# canabis_url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=100'
    
# r = requests.get(canabis_url)
# data = r.json()
# data


# In[3]:


# # посмотрим, что тут лежат за данные
# # всего сто строк, id и uid уникальные значения
# data_df = pd.DataFrame(data)


# In[4]:


# создаем функцию get_staff. в ней обращаемся к API, устанавливаем коннет с базой, которая мы развернули ранее вместе с airflow 
# images. создавая conn используем библиотеку psycopg2 и стандартные данные для подключения, которые указаны 
# в контейнере airflow_images-postgres-1. создаем нужные нам поля и делаем вставку. далее определяем нашу функцию в dag и
# сохраняем этот файл в рабочую дикерторию airflow 

def get_staff():
    response = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=100')
    data = response.json()
    
    conn = psycopg2.connect(
        host='localhost',
        port='5432',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cannabis (
            id INTEGER PRIMARY KEY,
            uid VARCHAR(255),
            strain VARCHAR(255),
            cannabinoid_abbreviation VARCHAR(255),
            terpene VARCHAR(255),
            medical_use VARCHAR(255),
            health_benefit VARCHAR(255),
            type VARCHAR(255),
            buzzword VARCHAR(255),
            brand VARCHAR(255),
            getdata_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ON CONFLICT (id) DO NOTHING
            )''')
    
    for item in data:
        cursor.execute('''
            INSERT INTO cannabis (id, uid, strain, cannabinoid_abbreviation, terpene, medical_use, health_benefit, 
            type, buzzword, brand, getdata_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DEFAULT)
            ''', (item['id'], item['uid'], item['strain'], item['cannabinoid_abbreviation'], item['terpene'], item['medical_use'], item['health_benefit']
                  , item['type'], item['buzzword'], item['brand']))
    conn.commit()
    cursor.close()
    conn.close()


# In[5]:


default_args = {
    'owner': 'aero',
    'start_date': datetime(2023, 7, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    schedule=timedelta(hours=12),
)

get_staff_data = PythonOperator(
    task_id='get_staff_task',
    python_callable=get_staff,
    dag=dag,
)

t1 = PythonOperator(task_id='start_task', python_callable=get_staff, dag=dag)


# In[ ]:




