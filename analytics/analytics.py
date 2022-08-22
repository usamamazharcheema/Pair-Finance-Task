from ctypes import cast
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from datetime import datetime
import sqlalchemy as db
import pandas as pd


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

print('Extracting the data from PosgresSQL and Performing Aggregations')

sql_query = """  
                WITH Cte_distance
                    AS (
                        SELECT device_id, hours, latA, longA, latB, longB, temperature,
                               st_distance(st_point(longA, latA), st_point(longB,latB)) AS distance_in_m 
                        FROM ( 
                                SELECT device_id, hours, temperature, latA, longA, 
                                       LAG(latA) OVER (PARTITION BY device_id, hours) AS latB, 
                                       LAG(longA) OVER (PARTITION BY device_id, hours) AS longB 
                                FROM (
                                        SELECT device_id, temperature, 
                                               EXTRACT(hour from to_timestamp(TRUNC(CAST(time AS bigint)))) as hours, 
                                               (location::json->>'latitude')::float AS latA, 
                                               (location::json->>'longitude')::float AS longA 
                                        FROM devices
                                      ) t_hours_lat_long
                                                 
                             ) t_lag_by_partition )  
      
                SELECT device_id, hours, 
                       MAX(temperature) as max_temperature, 
                       COUNT(device_id) as total_datapoints,
                       SUM(distance_in_m) AS total_distance_in_meter
                FROM Cte_distance
                GROUP BY device_id, hours

            """
try:
    df_agg_all = pd.read_sql_query(sql_query , con=psql_engine)
except (OperationalError, ProgrammingError) as e:
    print('Error occured while pulling data from PostgreSQL {}'.format(e.args))
print('Data pulled from PostgresSQL and Aggregations are done')

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')

df_agg_all.head(n=0).to_sql(name = "device_agg_all", con=mysql_engine, if_exists='replace', index=False)
try:
    df_agg_all.to_sql(name = 'device_agg_all', con=mysql_engine, if_exists='append', index=False)
except (OperationalError, ProgrammingError) as e:
    print('Error occured while inserting data in MySQL {}'.format(e.args))
print('Data inserted to MySQL')

try:
    df_msql_agg_all = pd.read_sql('SELECT * FROM device_agg_all', con=mysql_engine)
except (OperationalError, ProgrammingError) as e:
    print('Error occured while pulling data from MySQL {}'.format(e.args))
print('Data pulled From MySQL')
print(df_msql_agg_all)