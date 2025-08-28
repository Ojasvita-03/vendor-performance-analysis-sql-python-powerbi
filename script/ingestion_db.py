import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Ensure logs folder exists
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create database engine
engine = create_engine('sqlite:///inventory.db')

# Function to ingest DataFrame into database
def ingest_db(df, table_name, engine):
    '''This function ingests the DataFrame into a database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Function to load CSVs and ingest into DB
def load_raw_data():
    '''This function loads CSVs as DataFrames and ingests them into the DB'''
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join('data', file))
            logging.info(f'Ingesting {file} into database')
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('------------Ingestion Complete-------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

# Run the ingestion process
if __name__ == '__main__':
    load_raw_data()
