from time import asctime
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.ETLconfigs import *
from Utilities.Utils import *
import logging

logging.basicConfig(
    filename = "Logs/ETLLogs.log",
    filemode = "a",
    format = "%(asctime)s-%(levelname)s-%(message)s",
    level = logging.INFO
)
logger = logging.getLogger(__name__)

oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", echo=False, future=True)

# Creating a Class
class DataExtraction:

    def extract_sales_data(self,file_path,file_type,table_name,db_engine):
        logger.info("Sales data extraction started...")
        try:
            CommomUtilities.read_files_and_write_to_stage(file_path,file_type,table_name,db_engine)   # Line 27
            logger.info("Sales data extraction completed")
        except Exception as e:
            logger.error(f"Error while sales data extraction: {e}", exc_info=True)                    # Line 30

    def extract_product_data(self, file_path, file_type, table_name, db_engine):
        logger.info("Product data Extraction Started..")
        try:
            CommomUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)  # Line 35
            logger.info("Product data extraction Completed..")
        except Exception as e:
            logger.error(f"Error while product data extraction: {e}", exc_info=True)                   # Line 38

    def extract_supplier_data(self, file_path, file_type, table_name, db_engine):
        logger.info("Supplier data Extraction Started..")
        try:
            CommomUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)  # Line 43
            logger.info("Supplier data extraction Completed..")
        except Exception as e:
            logger.error(f"Error while supplier data extraction: {e}", exc_info=True)                  # Line 46

    def extract_inventory_data(self, file_path, file_type, table_name, db_engine):
        logger.info("Inventory data Extraction Started..")
        try:
            CommomUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)  # Line 51
            logger.info("Inventory data extraction Completed..")
        except Exception as e:
            logger.error(f"Error while inventory data extraction: {e}", exc_info=True)                 # Line 54

    def extract_stores_data(self, table_name, oracle_engine, mysql_engine):
        logger.info("Stores data Extraction Started..")
        try:
            df = pd.read_sql("SELECT * FROM stores", oracle_engine)
            df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)
            logger.info("Stores data extraction Completed..")
        except Exception as e:
            logger.error(f"Error while stores data extraction: {e}", exc_info=True)                    # Line 62

# Creating a main method
if __name__ == "__main__":
    # pre-reqisite ( extract sales_data_tobedeleted.csv file from Linux server )
    logger.info("file getting extracted ...")
    pre_req_ref = CommomUtilities()
    # download the linux file to local/project
    pre_req_ref.Sales_Data_From_Linux_Server()
    logger.info("file getting completed ...")

    extRef = DataExtraction()  # Creating an Object inside main method
    extRef.extract_sales_data("Source_Systems/sales_data_from_Linux_remote.csv","csv","staging_sales",mysql_engine)
    extRef.extract_product_data("Source_Systems/product_data.csv","csv","staging_product", mysql_engine)
    extRef.extract_supplier_data("Source_Systems/supplier_data.json","json","staging_supplier", mysql_engine)
    extRef.extract_inventory_data("Source_Systems/inventory_data.xml","xml","staging_inventory", mysql_engine)
    extRef.extract_stores_data("staging_stores", oracle_engine, mysql_engine)
