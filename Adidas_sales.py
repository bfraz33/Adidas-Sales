import pandas as pd
import yaml
import logging
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData


def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
        return config
def write_to_xlsx(file_path, data):
    data.to_excel(file_path, index=False)
def extract_data(file_path):
    selected_columns = ['Retailer ID', 'Retailer', 'Region', 'State', 'City', 'Product', 'Price per Unit', 'Units Sold', 'Total Sales', 'Sales Method',
                        'Invoice Date']
    data = pd.read_excel(file_path, index_col=None, header=4, usecols=selected_columns)
    print(data.columns)
    return data
def transform_data(data):
    columns_to_round = ['Price per Unit', 'Total Sales']  # Add more columns if needed
    data[columns_to_round] = data[columns_to_round].round(0).astype(int)

    if 'Invoice Date' in data.columns:

        data['Invoice Date'] = pd.to_datetime(data['Invoice Date'], errors='coerce')

        data['Year'] = data['Invoice Date'].dt.year

    return data

def configure_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
def load_data_to_database(engine, table_name, data):
   data.to_sql(table_name, engine, if_exists='replace', index=False)
   logging.info("ETL process completed.")

def create_database_schema(engine):
    metadata = MetaData()
    Sales_data = Table(
        'Sales_data', metadata,
        Column('Retailer ID', Integer, primary_key=True),
        Column('Retailer', String),
        Column('Region', String),
        Column('State', String),
        Column('City', String),
        Column('Product', String),
        Column('Price per Unit', Float),  # Adjust data type if needed
        Column('Units Sold', Integer),
        Column('Total Sales', Float),  # Adjust data type if needed
        Column('Sales Method', String),
        Column('Invoice Date')
    )

    metadata.create_all(engine)

    return metadata

if __name__ == "__main__":
    # Load configuration
    config = load_config('config.yaml')
    excel_file_path = config['excel_file_path']
    log_file_path = config['log_file_path']
    db_config = config['db_config']

    # Extract
    data = extract_data(excel_file_path)

    # Transform
    transformed_data = transform_data(data)

    # Load to CSV (optional)
    write_to_xlsx('transformed_data.xlsx', transformed_data)

    # Configure logging
    configure_logging(log_file_path)

    # Create database engine
    engine = create_engine(
        f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
    )

    # Create database schema

    # Load to database
    load_data_to_database(engine, 'Sales_data', transformed_data)





