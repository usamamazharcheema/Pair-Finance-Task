## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.

ETL Script is written in analytics.py file which is pulling data from PostrgresSQL, performing aggregations
and then inserting data into MySQL and then pulling it back.