## DATA ENGINEERING PROJECT ON GOOGLE CLOUD: LOL RANKED GAMES

### DESCRIPTION
The application gets matches data from the official API, transforms data, and puts it in a Data Lake/Data Warehouse. Then we perform some Explanatory Data Analysis. The main goal is to deliver game knowledge to players through statistical elements and charts to help them get better in games.

### STEP TO RUN
- Create your Riot Development (or Production, if you can) key. See: https://developer.riotgames.com/app-type
- Create a Google service account and download the json file.
- Create the bucket on Google Cloud Storage (the project push data on GCS)
- Rename `ingestion\common.example.py`. to `ingestion\common.py`. and fill out the variables in this file.

- If you use Docker:
    - Build the image: `docker build -t lol_de:1.0 .`
    - Run the image: `docker run -t lol_de:1.0`
- If you run directly on your host
    - Create venv and activate a new venv: https://docs.python.org/3/library/venv.html
    - Install requirements: `pip install -r requirements.txt`
    - Run program: `python ingestion\pipeline.py`

### NEXT STEPS
- Migrate the code to dags to run the code on Airflow
- Batch load csv files from Google Cloud Storage to BigQuery for futher analysis and data visualizations
