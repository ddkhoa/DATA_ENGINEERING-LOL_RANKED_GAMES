## DATA ENGINEERING PROJECT ON GOOGLE CLOUD: LOL RANKED GAMES

### DESCRIPTION
The application gets matches data from the official API, transforms data, and puts it in a Data Lake/Data Warehouse. Then it performs some Explanatory Data Analysis. The main goal is to deliver game knowledge to players through statistical elements and charts to help them get better in games.

Some charts that I created based on the data: https://public.tableau.com/app/profile/khoa8102/viz/LOL_RANKED_GAME_VIZ/Dashboard2?publish=yes

### STEP TO RUN
- Create your Riot Development (or Production, if you can) key. See: https://developer.riotgames.com/app-type
- Create a Google service account and download the json file.
- Create the bucket on Google Cloud Storage (the project push data on GCS)
- Rename `ingestion\dependencies\common.example.py`. to `ingestion\dependencies\common.py` and fill out the variables in this file.

- If you use Docker:
    - Build the image: `docker build -t lol_de:1.0 .`
    - Run the image: `docker run -t lol_de:1.0`
- If you run directly on your host
    - Create venv and activate a new venv: https://docs.python.org/3/library/venv.html
    - Install requirements: `pip install -r requirements.txt`
    - Run program: `python ingestion\pipeline.py`
- If you want to run on Apache Airflow
    - Pull the Airflow docker image: `docker pull puckel/docker-airflow`
    - Run this command: `docker run -d -p 8080:8080 -v %cd%/ingestion:/usr/local/airflow/dags -v %cd%/requirements.txt:/requirements.txt puckel/docker-airflow webserver`. (Repace %cd% by $pwd for Linux)
    - You can also upload the folder `ingestion` to your Google Composer DAGs folder to run the pipeline on Google Cloud.

### NEXT STEPS
- Batch load csv files from Google Cloud Storage to BigQuery for futher analysis and data visualizations

### TECHNOLOGIES
- Python, Docker, Google Cloud Platform (Storage, BigQuery), Airflow, Tableau