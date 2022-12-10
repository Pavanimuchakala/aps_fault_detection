import pandas as pd
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import os,sys
import yaml

def get_collection_as_dataframe(database_name:str,collection_name:str)-> pd.DataFrame:
    """
    Description: This function returns the mongoDB collection as dataframe
    Params:
    database_name: database name
    collection_name: collection name
    ================================================
    returns Pandas dataframe of collection
    """
    try:
        logging.info(f"Reading data from Database: {database_name} and collection{collection_name}")
        df = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f"Found Columns: {df.columns}")
        if "_id" in df.columns:
            logging.info(f"Dropping __id column")
            df.drop("_id", axis=1, inplace=True)

        return df
    except Exception as e:
        raise SensorException(e, error_details=sys)


def write_yaml_file(file_path, data:dict):
    """
    Description: This function writes the given data to yaml file
    params:
    file_path : Directory to save the file
    data: data to be written in the dict format
    """
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir,exist_ok=True)
        with open(file_path,"w") as file_writer:
            yaml.dump(data,file_writer)


    except Exception as e:
        raise SensorException(e, error_details=sys)


def convert_columns_float(df,exclude_columns:list):
    try:

        for column in df.columns:
            if column not in exclude_columns:
                df[column] = df[column].astype('float')
        
        return df

    except Exception as e:
        raise SensorException(e, error_details=sys)