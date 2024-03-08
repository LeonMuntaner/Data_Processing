import os
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def _load_data_from_excel(file_paths):
    df_main = {}
    
    for file_path in file_paths:
        # read the excel file
        xl_file = pd.ExcelFile(file_path)
        
        # loop through each sheet name in the file
        for sheet_name in xl_file.sheet_names:
            # read the sheet data into a dataframe
            df = xl_file.parse(sheet_name)
            
            # create a key for the dataframe using the sheet name
            key = sheet_name
            
            # add the dataframe to the dictionary with the key as the key
            df_main[key] = df
    return df_main

def preprocess_data_from_excel(file_path, data_folder, country_month):
    try:
        excel_data = _load_data_from_excel(file_path)
        
        # These are the sheets in the excel file
        df_1 = excel_data['Patient Case']
        df_2 = excel_data['Microscopy']
        df_3 = excel_data['Culture']
        df_4 = excel_data['Specimen']
        # df_5 = excel_data['Drug Susceptibility Testing']
        # df_6 = excel_data['Regimen-Treatment']
        # df_7 = excel_data['Biochemistry']
        
        dataframes = [df_1, df_2, df_3, df_4]
        timestamp = datetime.now().strftime("%Y%m%d")
        
        for idx, excel_data in enumerate(dataframes, start=1):
            csv_file_name = f'{country_month}_{idx}_{timestamp}.csv'
            csv_file_path = os.path.join(data_folder, csv_file_name) 
            excel_data.to_csv(csv_file_path, index=False)
    except Exception as e:
        logger.error(f'Preprocess data from excel function failed with error: {e}', exc_info=True)
        raise e

def preprocess_legacy_data_from_excel(file_path, file_number, month, target_sheet):
    try:
        excel_data = _load_data_from_excel(file_path)
    except Exception as e:
        logger.error(f'Load data from excel function failed with error: {e}', exc_info=True)
        raise e
    
    df_month = excel_data[target_sheet]
    
    dataframes = [df_month]
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    data_folder = r'C:\Users\muntanerl2\Documents\AVID-projects\Code\DEPOT-Code\Bulk-Upload-Dev\COUNTRIES\GEORGIA\data\raw'
    
    for idx, excel_data in enumerate(dataframes, start=1):
        csv_file_name = f'Georgia_legacy_{file_number}_{idx}_{month}_{timestamp}.csv'
        csv_file_path = os.path.join(data_folder, csv_file_name) 
        excel_data.to_csv(csv_file_path, index=False)