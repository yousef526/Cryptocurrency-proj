# Cryptocurrency-proj
A simple pipeline that pulls datat from an api, put it kafka producer then pull it using PySpark and dump data after processing Postgres DB 

# Infrastrucure
It uses docker-compose to build the solution 
It installs airflow with postgres DB, zookeeper, kafka and kafdrop UI to monitor the topics 

# Instructions
- create new folder\
- create inside the new folder other three folder with names (dags, plugins, logs)\

- write the docker command\
    **docker-compose up -d**\
- Open airflow ui and run the main dag
