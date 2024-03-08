import re
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# * This function iterates through the columns and transforms the values into what I want. 
# * It also handles cases where there might be NaN values. 
def _modify_value(value, col_name):
    if pd.notna(value):
        return '{' + col_name + '}'
    else:
        return '{' + col_name + '},NA'

# * This function will go through and filter out the NA portions from the string
def _filter_na(value):
    parts = value.split(" ")
    return " ".join([part for part in parts if "NA" not in part])

# * Now that I have the drugs column in a better format, I need to wrap everything with {} and a , separating each value.
def _concatenate_values(text):
    values = ','.join(re.findall(r'\{([^}]+)\}', text))
    return f'{{{values}}}'

def transform_regimen_treatment(regimen_treatment_df):
    try:
        regimen_treatment_df.rename(columns={'patient_local_identifier':'identifier', 
                            'start date':'start', 
                            'end date':'end',
                            'reason for ending':'reason'}, inplace=True)
        
        regimen_treatment_df['outcome'] = regimen_treatment_df['outcome'].replace(
            to_replace=['Still on treatment', 'Died', 'Cured', 'Completed', 'Lost to follow up', 'Lost to followup', 'Unknown', 'Failure', 'Palliative Care'],
            value=['stillOnTreatment', 'died', 'cured', 'completed', 'ltfu', 'ltfu', 'unknown', 'failure', 'palliativeCare']
        )
        
        regimen_treatment_df['reason'] = regimen_treatment_df['reason'].replace(
            to_replace=['Treatment ended', 'Adverse event', 'Treatment ineffective due to additional resistance', 'Resistance', 'Continuation of treatment', 
                        'New drugs available', 'Patient stopped treatment', 'Terminated from study', 'Drug(s) no longer available'],
            value=['ended', 'adverse', 'resistance', 'resistance', 'continuation', 'newDrugAvailable', 'patientStoppedTreatment', 'terminated', 'notAvailable']
        )
        
        regimen_treatment_df['start'] = pd.to_datetime(regimen_treatment_df['start'])
        regimen_treatment_df['start'] = regimen_treatment_df['start'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        regimen_treatment_df['end'] = pd.to_datetime(regimen_treatment_df['end'])
        regimen_treatment_df['end'] = regimen_treatment_df['end'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info('Finished transforming regimen_treatment_df')
        return regimen_treatment_df
    except Exception as e:
        logger.error(f'Error in transform_regimen_treatment: {e}')
        raise e

def transform_regimen(regimen_df, registration_df):
    try:
        columns_with_nan = regimen_df.columns[regimen_df.isna().sum() == len(regimen_df)]
        regimen_df.drop(columns_with_nan, axis=1, inplace=True)
        
        # def modify_value(value, col_name):
        #     if pd.notna(value):
        #         return '{' + col_name + '}'
        #     else:
        #         return '{' + col_name + '},NA'
        
        # List of columns to apply the function
        cols_to_modify = regimen_df.columns[4:].tolist()
        
        # Apply the function to each value in the specified column 
        for col in cols_to_modify:
            regimen_df[col] = regimen_df[col].apply(lambda x: _modify_value(x, col))
        
        # This creates the drugs column with the joined data
        regimen_df['drugs'] = regimen_df.iloc[:, 4:].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        
        # Keep only these desired columns
        cols_to_keep = ['identifier', 'start', 'end', 'reason', 'drugs']
        regimen_df = regimen_df[cols_to_keep]
        
        # def filter_na(value):
        #     parts = value.split(" ")
        #     return " ".join([part for part in parts if "NA" not in part])
        
        regimen_df['drugs'] = regimen_df['drugs'].apply(_filter_na)
        
        # Now that I have the drugs column in a better format, I need to wrap everything with {} and a , separating each value.
        # def concatenate_values(text):
        #     values = ','.join(re.findall(r'\{([^}]+)\}', text))
        #     return f'{{{values}}}'
        
        regimen_df['drugs'] = regimen_df['drugs'].apply(_concatenate_values)
        
        regimen_df = pd.merge(regimen_df, registration_df, on='identifier')
        
        regimen_df['registrationdate'] = pd.to_datetime(regimen_df['registrationdate'])
        regimen_df['registrationdate'] = regimen_df['registrationdate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Re-order the columns to match how they look in the database.
        new_order = ['identifier', 'registrationdate', 'start', 'end', 'reason', 'drugs']
        regimen_df = regimen_df.reindex(columns=new_order)
        
        logger.info('Finished transforming regimen_df')
        return regimen_df
    except Exception as e:
        logger.error(f'Error in transform_regimen: {e}')
        raise e