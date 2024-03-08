# import csv
import os
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def _drop_example_row(dataframe, rows_to_drop=None):
    try:
        if rows_to_drop is not None:
            if isinstance(rows_to_drop, int):
                rows_to_drop = [rows_to_drop] #convert single index to a list
            
            if isinstance(rows_to_drop, list):
                dataframe = dataframe.drop(index=rows_to_drop).reset_index(drop=True)
        
        logger.info(f"Rows dropped: {rows_to_drop}")
        return dataframe
    except Exception as e:
        logger.error(f"Error dropping rows: {e}")
        raise e

def _transform_containerid(row):
    try:
        identifier = row['patient_local_identifier']
        bodysite = row['collection_bodysite']
        collected = row['collected_date']
        
        # Convert collected to datetime format
        collected = pd.to_datetime(collected, infer_datetime_format=True)
        
        # Now, format the dates into the target format of %Y-%m-%d %H:%M:%S
        collected = collected.strftime('%Y-%m-%d %H:%M:%S')
        
        # Determine the letter based on bodysite
        letter = bodysite[0].upper()
        
        # Format the final value
        transformed_value = f"{identifier}_{letter}_{collected[8:10]}{collected[5:7]}{collected[2:4]}"
        
        logger.info(f"Transformed containeridentifier: {transformed_value}")
        return transformed_value
    except Exception as e:
        logger.error(f"Error transforming containeridentifier: {e}")
        raise e

def _is_column_data_valid(df):
    column_constraints = {
        'collection_bodysite': ['biopsy', 'sputum', 'other', 'surgeryCaseousMasses', 'surgeryCavityInternalWall', 'surgeryCavityExternalWall', 'surgeryModulu',
                                'surgeryHealthy tissue', 'bronchialLavage', 'asciticFluid', 'blood', 'urine', 'pleuralFluid', 'cerebrospinalFluid', 'paraffinEmbeddedTissue'],
        
        'value': ['positive', 'negative', 'unknownData'],
        'result': ['-0.09', '1+', '2+', '3+', '4+', 'saliva', 'exclude', 'unknownData']
    }
    
    invalid_data = {}
    
    # Check if the data in the columns are valid and save the dataframes if they are.
    for column_name, accepted_values in column_constraints.items():
        invalid_values = df[~df[column_name].isin(accepted_values)][column_name].tolist()
        if invalid_values:
            invalid_data[column_name] = invalid_values
    
    # If there is invalid data, raise an error and print the column name and the invalid values
    if invalid_data:
        message = "Invalid data found in the following columns:"
        for column_name, invalid_values in invalid_data.items():
            message += f"\n{column_name}: {', '.join(map(str, invalid_values))}"
        raise ValueError(message)

def _subset_and_save_dataframes(dataframe, file_number, month):
    try:
        # Now that the dataframe is mostly cleaned, I will make the specimen, culture, and microscopy dataframes-
        # by subsetting dataframe1_v2
        specimen = dataframe[['patient_local_identifier', 'registrationdate', 'spec_local_identifier', 'collected_date', 'collection_bodysite']]
        
        # The default value for culturetype is liquid
        culture = dataframe[['patient_local_identifier', 'registrationdate', 'spec_local_identifier', 'collected_date', 'value']]
        culture['culturetype'] = 'liquid'
        
        # The default value for microscopytype is florescence
        microscopy = dataframe[['patient_local_identifier', 'registrationdate', 'spec_local_identifier', 'collected_date', 'result']]
        microscopy['microscopytype'] = 'florescence'
        
        dataframes = [specimen, culture, microscopy]
        
        # Change the folder depending on the country 
        data_folder = 'Georgia\data\intermediate'
        
        index_labels = {1: 'specimen', 2: 'culture', 3: 'microscopy'}
        
        # Timestamp for unique file names
        # timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Loop through the dataframes list and save them 
        for idx, df in enumerate(dataframes, start=1):
            csv_file_name = f'Georgia_{file_number}_{idx}_{month}'
            
            # Conditionally add the label based on the index
            if idx in index_labels:
                csv_file_name += f'_{index_labels[idx]}'
            
            csv_file_name += '.csv'
            
            csv_file_path = os.path.join(data_folder, csv_file_name)
            df.to_csv(csv_file_path, index=False)
        
        logger.info(f"Dataframes saved successfully")
    except Exception as e:
        logger.error(f"Error subsetting and saving dataframes: {e}")
        raise e

def preprocess_Georgia_data(dataframe1, registrationdate, rows_to_drop, file_number, month):
    # This drops the rows that I don't need
    dataframe1 = _drop_example_row(dataframe1, rows_to_drop)
    
    # Slicing columns that I don't need
    dataframe1 = dataframe1.loc[:, 'Unnamed: 0':'Unnamed: 10']
    
    # Setting this index value to be patient_local_identifier so that when I make the columns the names of the first row. 
    dataframe1.at[0, 'Unnamed: 0'] = 'patient_local_identifier'
    
    # Normalize the value name. I noticed that in the source file this value tends to change.
    replacement_dict = {'Mizezi': 'Normalized', 'Aim': 'Normalized', 'TB type': 'TB Type', 'Culture number': 'Culture #',
                                        'Culture Number': 'Culture #','Culture result': 'FinalR', 'Microscopy result': 'Microscopy'}
    
    # Iterate through the dataframe and apply the normalized replacement
    for column in dataframe1.columns:
        dataframe1[column] = dataframe1[column].replace(replacement_dict, regex=True)
    
    # Made the values of the first row the column names 
    dataframe1.columns = dataframe1.values[0]
    dataframe1 = dataframe1.reset_index(drop=True)
    
    # Dropped the row with the names
    dataframe1.drop([0], inplace = True)
    dataframe1.reset_index(drop=True, inplace=True)
    
    dataframe1.dropna(how='all', inplace=True)
    dataframe1.reset_index(drop=True, inplace=True)
    
    # This drops the rows that have the column names in them
    dataframe1_v2 = dataframe1[dataframe1['Normalized'] != 'Normalized']
    dataframe1_v2.reset_index(drop=True, inplace=True)
    
    # Parth told me these columns could also be dropped
    dataframe1_v2.drop(['Normalized', 'DSTResult', 'TB Type', 'Month', 'Origin'], axis=1, inplace=True)
    
    # Now I need to start cleaning the data and transforming it into the target structure
    dataframe1_v2['patient_local_identifier'].ffill(axis=0, inplace=True)
    
    # Changing the column names to what the other source files have them as.
    dataframe1_v2.rename(columns={
        'Specimen':'collection_bodysite',
        'Culture #':'spec_local_identifier', 
        'Microscopy':'result',
        'Culture date':'collected_date',
        'FinalR':'value'}, inplace=True)
    
    # This cleaning process follows the usual one except some of the source names are different
    dataframe1_v2['result'] = dataframe1_v2['result'].replace(
            to_replace=['AFB (-)', 'AFB(-)', 'AFB (-) ნერწყვი', 'AFB (1+)', 'AFB(1+)', 'AFB (2+)', 'AFB(2+)', 'AFB (3+)', 'AFB(3+)', 
                        'AFB (4+)', 'AFB(4+)', '1-3 AFB', '1-3 მგბ', 'AFB(-) saliva', 'AFB (-) Saliva', '-'],
            
            value=['-0.09', '-0.09', '-0.09', '1+', '1+', '2+', '2+', '3+', '3+', '4+', '4+', '1+', '1+', 'saliva', 'saliva', 'exclude']
    )
    
    # Replace the nan values in the result column with 'unknownData'
    dataframe1_v2['result'].fillna('unknownData', inplace=True)
    
    dataframe1_v2['collection_bodysite'] = dataframe1_v2['collection_bodysite'].replace(
        to_replace=['Biopsy', 'Sputum', 'Other', 'Surgery-caseous masses', 'Surgery-cavity internal wall', 'Surgery-cavity external wall', 
                    'Surgery-infectious granuloma', 'Surgery-healthy tissue', 'Bronchial lavage', 'Ascitic fluid', 'Ascitis fluid', 'Blood', 'Urine', 'Pleural fluid',
                    'Cerebrospinal Fluid', 'Paraffin Embedded Tissue', 'SAB (aspirat bronhic)', 'Saliva', 'Tracheal aspirate', 
                    'BAL', 'Surgery', 'Stool', 'Soft tissue puncture', 'Joint puncture', 'Pus', 'Surgery ', 'თავზურგტვინის სითხე', 'Bronchial wash',
                    'Surgery sample', 'thyroid puncture', 'Spinal fluid', 'Lymph node', 'განავალი', 'ასციტური სითხე', 'Lung surgery', 'lymph node', 'spinal fluid',
                    'Lung tissue', 'Menstrual blood', 'Sperm', 'Surgery specimen', 'Vaginal smear', 'Fluid from Duglas space', 'ლიმფური კვანძიდან მასალა', 
                    'discharge from the pleural cavity', 'შარდი', 'Urine-sperma'],
        
        value=['biopsy', 'sputum', 'other', 'surgeryCaseousMasses', 'surgeryCavityInternalWall', 'surgeryCavityExternalWall', 'surgeryModulu',
                'surgeryHealthy tissue', 'bronchialLavage', 'asciticFluid', 'asciticFluid', 'blood', 'urine', 'pleuralFluid', 'cerebrospinalFluid', 'paraffinEmbeddedTissue', 'other', 'other', 'other',
                'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other',
                'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other', 'other']
    )
    
    dataframe1_v2['value'] = dataframe1_v2['value'].replace(
        to_replace=['MTBC', 'NEG', 'CON', 'WAT', 'მიღებულია ზრდა', 'Growth', 'Not enough growth for identification'],
        value=['positive', 'negative', 'unknownData', 'unknownData', 'unknownData', 'unknownData', 'unknownData']
    )
    
    dataframe1_v2 = pd.merge(dataframe1_v2, registrationdate, on='patient_local_identifier')
    
    # Convert collected to datetime format
    dataframe1_v2['collected_date'] = pd.to_datetime(dataframe1_v2['collected_date'], infer_datetime_format=True)
    
    # Now, format the dates into the target format of %Y-%m-%d %H:%M:%S
    dataframe1_v2['collected_date'] = dataframe1_v2['collected_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Apply the function to create a new column with the transformed values
    dataframe1_v2['transformed_value'] = dataframe1_v2.apply(_transform_containerid, axis=1)
    
    # Update the spec_local_identifier column with the transformed values
    dataframe1_v2['spec_local_identifier'] = dataframe1_v2['transformed_value']
    
    # Drop the transformed_value column
    dataframe1_v2 = dataframe1_v2.drop(columns=['transformed_value'])
    
    # Check and save the dataframes
    try:
        _is_column_data_valid(dataframe1_v2)
        _subset_and_save_dataframes(dataframe1_v2, file_number, month)
    except ValueError as e:
        print(e)

def transform_column_names(file_path):
    try:
        processed_dir = 'Georgia\data\processed'
        
        # Extract the file name without extension
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        
        # Define column name mappings based on the file name pattern
        if 'specimen' in file_name:
            column_name_mapping = {
                'patient_local_identifier':'identifier', 
                'spec_local_identifier':'containeridentifier', 
                'collected_date':'collected', 
                'collection_bodysite':'bodysite'
            }
        elif 'culture' in file_name:
            column_name_mapping = {
                'patient_local_identifier':'identifier', 
                'spec_local_identifier':'containeridentifier', 
                'collected_date':'issued'
            }
        elif 'microscopy' in file_name:
            column_name_mapping = {
                'patient_local_identifier':'identifier', 
                'spec_local_identifier':'containeridentifier', 
                'collected_date':'issued',
                'result':'value'
            }
        
        df = pd.read_csv(file_path)
        
        # Rename column based on the mapping
        df.rename(columns=column_name_mapping, inplace=True)
        
        # Save the processed CSV files in the processed directory
        output_path = os.path.join(processed_dir, os.path.basename(file_path))
        df.to_csv(output_path, index=False)
        
        logger.info(f"Column names transformed successfully")
    except Exception as e:
        logger.error(f"Error transforming column names: {e}")
        raise e


# * I made this function so that the process above would be more DRY
# * This function cut down the code and creates the dataframes for the condition and patient table in one run
def create_patient_condition_table(df, CB_prod_data):
    try:
        patient = CB_prod_data[['patient_local_identifier', 'gender', 'managingorganization_id']]
        condition = CB_prod_data[['patient_local_identifier', 'casedefinition', 'ageonset', 'registrationdate']]
        
        patient.rename(columns={
            'patient_local_identifier':'identifier',
            'managingorganization_id':'managingorganizationid'}, inplace=True)
        
        condition.rename(columns={'patient_local_identifier':'identifier'}, inplace=True)
        
        patient_df = pd.merge(df, patient, on='identifier')
        
        patient_df = patient_df[['gender', 'managingorganizationid', 'identifier']]
        
        patient_df.drop_duplicates(inplace=True)
        patient_df.reset_index(drop=True, inplace=True)
        
        condition_df = pd.merge(df, condition, on='identifier')
        condition_df = condition_df[['identifier', 'casedefinition', 'ageonset', 'registrationdate_x']]
        
        condition_df.drop_duplicates(inplace=True)
        condition_df.reset_index(drop=True, inplace=True)
        
        condition_df.rename(columns={'registrationdate_x':'registrationdate'}, inplace=True)
        
        logger.info(f"Patient and condition tables created successfully")
        return patient_df, condition_df
    except Exception as e:
        logger.error(f"Error creating patient and condition tables: {e}")
        raise e