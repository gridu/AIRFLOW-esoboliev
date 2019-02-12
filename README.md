# Apache Airflow Grid U project

Apache Airflow is a workflow system for managing and scheduling data pipelines.
Airflow allows you to programmatically author, schedule, and monitor workflows.
You can easily visualize your data pipelines’ dependencies, progress, logs, code, trigger tasks, and success status.

# How to run this application

1. Create virtul env for this project _virtualenv -p python venv_.
2. Start working in your env _source ./venv/bin/activate_.
3. Install hardcoded version of airflow to env _pip install -r ./requirements.txt_.
4. Set up _AIRFLOW_HOME_ environment variable.
5. Run _'airflow webserver'_.
6. Run _'airflow scheduler'_.

# How to setup PostgreSQL DB

1. Install PostgreSQL DB.
2. Create _airflow_db_ schema.
4. Create db connection with name _postgres_connection_ on UI.
