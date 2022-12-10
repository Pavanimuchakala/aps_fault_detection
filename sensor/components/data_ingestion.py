
from sensor import utils
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor.logger import logging
from sensor.exception import SensorException
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os,sys



class DataIngestion:
    
    def __init__(self,data_ingestion_config:config_entity.DataIngestionConfig):
        try:
            logging.info(f"{'>>'*20} Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
        
        except Exception as e:
            raise SensorException(e, sys)

    def initiate_data_ingestion(self)->artifact_entity.DataIngestionArtifact:
        try:
            logging.info(f"exporting collection data as dataframe")
            #Exporting data as dataframe
            df:pd.DataFrame = utils.get_collection_as_dataframe(
                database_name=self.data_ingestion_config.database_name,
                collection_name = self.data_ingestion_config.collection_name)
            
            #save data in featurestore
            df.replace(to_replace="na",value=np.NAN, inplace=True)

            logging.info(f"Create feature store folder if exists")
            #Create feature store folder
            feature_store_dir=os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir,exist_ok=True)

            #save df to feature store folder
            logging.info(f"save df to feature store folder")
            df.to_csv(path_or_buf=self.data_ingestion_config.feature_store_file_path,index=False,header = True)

            #split dataset into train and test 
            logging.info(f"split dataset into train and test ")
            train_df,test_df = train_test_split(df,test_size=self.data_ingestion_config.test_size,
            random_state=self.data_ingestion_config.random_state)

            #create dataset directory ifnot available
            logging.info("create dataset directory folder if not available")
            dataset_dir=os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(dataset_dir,exist_ok=True)

            #save df to feature store folder
            logging.info(f"save train_df and test_df to feature store folder ")
            train_df.to_csv(path_or_buf=self.data_ingestion_config.train_file_path,index=False,header = True)
            test_df.to_csv(path_or_buf=self.data_ingestion_config.test_file_path,index=False,header = True)

            #Prepare artifact

            data_ingestion_artifact=artifact_entity.DataIngestionArtifact(
                feature_store_file_path = self.data_ingestion_config.feature_store_file_path, 
                train_file_path = self.data_ingestion_config.train_file_path, 
                test_file_path = self.data_ingestion_config.test_file_path)
            logging.info(f"DataIngestionArtifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise SensorException(e,sys)