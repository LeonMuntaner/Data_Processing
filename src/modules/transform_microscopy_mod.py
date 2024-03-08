import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_microscopy(microscopy_df, registration_df):
    try:
        microscopy_df.rename(columns={'patient_local_Identifier':'identifier', 
                            'spec_local_identifier':'containeridentifier', 
                            'collected_date':'issued', 
                            'microscopy Type':'microscopytype',
                            'result':'value'}, inplace=True)
        
        # Transform the column values into what the data dictionary shows
        microscopy_df['value'] = microscopy_df['value'].replace(
            to_replace=['1 to 9 in 100 (1-9/100)', '1 to 9 in 100', '1 to 9 in 100 (-0.09)', '10 to 99 in 100 (1+)', '10 to 99 in 100(1+)', '1 to 9 in 1 (2+)', '10 to 99 in 1 (3+)', 
                        'More than 99 in 1 (4+)', 'Saliva', 'Unknown data', 'Negative', 'Not done', '1+ AFB', '2+ AFB', '3+ AFB', 'No AFB Observed'],
            value=['-0.09', '-0.09', '-0.09', '1+', '1+', '2+', '3+', '4+', 'saliva', 'unknownData', 'negative', 'notDone',
                    '1+', '2+', '3+', 'unknownData']
        )
        
        microscopy_df['value'] = microscopy_df['value'].fillna('unknownData')
        
        microscopy_df['microscopytype'] = microscopy_df['microscopytype'].str.strip()
        microscopy_df['microscopytype'] = microscopy_df['microscopytype'].replace(
            to_replace=['Ziehl-Neelsen', 'Zn', 'Flourescence', 'Not Specified', 'Not specified', 'Not Specified '],
            value=['zn', 'zn', 'florescence', 'notSpecified', 'notSpecified', 'notSpecified']
        )
        
        # Making the data type a string for column issued
        microscopy_df['issued'] = pd.to_datetime(microscopy_df['issued'])
        microscopy_df.issued = microscopy_df.issued.dt.strftime('%Y-%m-%d %H:%M:%S')
        
        microscopy_df = pd.merge(microscopy_df, registration_df, on='identifier')
        
        microscopy_df['registrationdate'] = pd.to_datetime(microscopy_df['registrationdate'])
        microscopy_df.registrationdate = microscopy_df.registrationdate.dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Re-order the columns to match how they look in the database.
        new_order = ['identifier', 'registrationdate', 'containeridentifier', 'issued', 'value', 'microscopytype']
        
        microscopy_df = microscopy_df.reindex(columns=new_order)
        
        logger.info("Microscopy table transformed successfully")
        return microscopy_df
    except Exception as e:
        logger.error(f"Error transforming microscopy table: {e}")
        raise e