#!/usr/bin/env python
# coding: utf-8

# # Import Libaries and Modules

# In[363]:


import pandas as pd
import json
import os
from glob import glob
import pandas.io.sql as psql
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine


# # Clone git Respository & Extract datafiles

# In[ ]:


def file_extraction():
    #download files    
    get_ipython().system('git clone https://github.com/annexare/Countries.git')
    
    #filepath
    path = os.path.join(os.getcwd(), 'Countries', 'data')
    
    return path


# In[289]:


path = file_extraction()


# In[291]:


def create_dataframe(file_path):
    import glob
    files = glob.glob(file_path + '/*.json')
    li = []
    
    for f in files:
        # read in json
        li.append(f)
        
    return li


# In[293]:


data_files = create_dataframe(path)


# In[294]:


data_files


# # Convert files to python dataframe

# In[338]:


#For Continents table
with open(data_files[0]) as continents:
    continents_file = json.load(continents)
    continents.close()
    

continents_df = pd.DataFrame(list(continents_file.items()), columns=['abrv', 'continent'])
    

#For countries_code table
with open(data_files[1]) as countries_code:
    countries_code_file = json.load(countries_code)
    countries_code.close()

country_code_df = pd.DataFrame(list(countries_code_file.items()), columns=['abrv', 'country_code'])


#For countries_code2 table
with open(data_files[2]) as countries_code2:
    countries_code_file2 = json.load(countries_code2)
    countries_code2.close()

country_code2_df = pd.DataFrame(list(countries_code_file2.items()), columns=['abrv', 'country_code'])

#For countries table
with open(data_files[3], 'rb') as countries:
    countries_file = json.load(countries)
    countries.close()
    
country_keys = []
country_values = []

for key, value in countries_file.items():
    country_keys.append(key)
    country_values.append(value)

country_df = pd.DataFrame(country_values, index=country_keys)
country_df['index'] = country_df.index
country_df.drop('continents', axis=1)


#For language table
with open(data_files[4], 'rb') as languages:
    languages_file = json.load(languages)
    languages.close()
    
languages_keys = []
languages_values = []

for key, value in languages_file.items():
    languages_keys.append(key)
    languages_values.append(value)

languages_df = pd.DataFrame(languages_values, index=languages_keys)
languages_df['index'] = languages_df.index


# In[351]:


country_df = country_df[['name','native','continent','capital','currency','languages','index']]


# # Create and Connect to DB

# In[305]:


def create_database():
    # enter databse name
    dbname = input("Enter your database: ")

    # enter password
    password = input("Enter your password: ")
    print(password)

    # establishing the connection
    conn = psycopg2.connect(
        database="postgres", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    conn.autocommit = True

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    sql1 = f'''DROP DATABASE IF EXISTS {dbname}'''
    cursor.execute(sql1)

    # Preparing query to create a database
    sql = f'''CREATE database {dbname}'''

    # Creating a database
    cursor.execute(sql)
    print("Database created successfully........")

    # Closing the connection
    conn.close()

    return dbname, password


# In[ ]:


dbname, password = create_database()


# # CREATE TABLES

# In[326]:


def continents(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS continents')

    # create continents if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS continents(
                      id SERIAL UNIQUE,
                        abrv text,
                    continent text
                    )'''

    cursor.execute(sql)
    print("continents created successfully........")

    conn.commit()

    cursor.close()
    print("tables created")


# In[332]:


def countries_code(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS countries_code')

    # create continents if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS countries_code(
                      id SERIAL UNIQUE,
                        abrv text,
                    country_code text
                    )'''

    cursor.execute(sql)
    print("country_code created successfully........")

    conn.commit()

    cursor.close()
    print("tables created")


# In[334]:


def countries_code2(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS countries_code2')

    # create continents if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS countries_code2(
                      id SERIAL UNIQUE,
                        abrv text,
                    country_code text
                    )'''

    cursor.execute(sql)
    print("country_code2 created successfully........")

    conn.commit()

    cursor.close()
    print("tables created")


# In[352]:


def countries(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS countries')

    # create continents if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS countries(
                      id SERIAL UNIQUE,
                        name text,
                    native text,
                    continent text,
                    capital text,
                    currency text,
                    languages text,
                    index text
                    
                    )'''

    cursor.execute(sql)
    print("countries created successfully........")

    conn.commit()

    cursor.close()
    print("tables created")


# In[356]:


def languages(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS languages')

    # create continents if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS languages(
                      id SERIAL UNIQUE,
                        name text,
                        native text,
                        rtl text,
                        index text
                    
                    )'''

    cursor.execute(sql)
    print("languages created successfully........")

    conn.commit()

    cursor.close()
    print("tables created")


# In[327]:


continents(dbname, password)


# In[333]:


countries_code(dbname, password)


# In[335]:


countries_code2(dbname, password)


# In[353]:


countries(dbname, password)


# In[357]:


languages(dbname, password)


# # Insert data into the tables

# In[328]:


def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


# In[329]:


conn = psycopg2.connect(
    database=dbname, user='postgres', password=password, host='127.0.0.1', port='5432'
)


# In[330]:


execute_values(conn, continents_df, 'continents')


# In[346]:


execute_values(conn, country_code_df, 'countries_code')


# In[347]:


execute_values(conn, country_code2_df, 'countries_code2')


# In[354]:


execute_values(conn, country_df, 'countries')


# In[358]:


execute_values(conn, languages_df, 'languages')


# # CREATE REPORTS

# In[373]:


engine = create_engine('postgresql+psycopg2://postgres:chrisadigwe1994@localhost/chris')


# In[378]:


connect = engine.connect()
dataid = 1022

query = '''select continents.continent, count(countries.name) 
            from countries
            inner join continents
            on countries.continent = continents.abrv
            GROUP BY continents.continent
            ORDER BY continents.continent

        '''


df = pd.read_sql_query(query, engine)


# In[379]:


df

