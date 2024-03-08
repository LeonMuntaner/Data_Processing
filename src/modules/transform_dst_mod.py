import json
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

def transform_dst(dst_df, registration_df):
    try:
        dst_df.rename(columns={'specimen':'identifier', 
                        'specimen local':'containeridentifier', 
                        'dst test':'dsttest',
                        'date':'issued'}, inplace=True)
        
        dst_df['dsttest'] = dst_df['dsttest'].replace(
            to_replace=['Bactec', 'FL-LPA', 'SL-LPA', 'Lowenstein-Jensen', 'GeneXpert', 'TRUENAT', 'DST'],
            value=['bactec', 'hain', 'lpaOther', 'le', 'genexpert', 'truenat', 'dst']
        )
        
        dst_df['issued'] = pd.to_datetime(dst_df['issued'])
        dst_df['issued'] = dst_df['issued'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # * Noticed that some of the columns were entirely missing. So I droppeed them.
        columns_with_nan = dst_df.columns[dst_df.isna().sum() == len(dst_df)]
        dst_df.drop(columns_with_nan, axis=1, inplace=True)
        
        # * This function iterates through the columns and transforms the values into what I want. 
        # * It also handles cases where there might be NaN values. 
        def modify_value(value, col_name):
            if pd.notna(value):
                return '({code:' + col_name + '},' + value + ')'
            else:
                return '({code:' + col_name + '},NA)'
        
        # * List of columns to apply the function
        cols_to_modify = dst_df.columns[4:].tolist()
        
        # * Apply the function to each value in the specified column 
        for col in cols_to_modify:
            dst_df[col] = dst_df[col].apply(lambda x: modify_value(x, col))
        
        # * This creates the drugs column with the joined data
        dst_df['drugs'] = dst_df.iloc[:, 4:].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        
        # Keep only these desired columns
        cols_to_keep = ['identifier', 'containeridentifier', 'dsttest', 'issued', 'drugs']
        dst_df = dst_df[cols_to_keep]
        
        # * This function will go through and filter out the NA portions from the string
        def filter_na(value):
            parts = value.split(" ")
            return " ".join([part for part in parts if "NA" not in part])
        
        dst_df['drugs'] = dst_df['drugs'].apply(filter_na)
        
        # * Now that I have the drugs column in a better format, I need to wrap everything with {} and a , separating each value.
        dst_df['drugs'] = '{' + dst_df['drugs'] + '}'
        dst_df['drugs'] = dst_df['drugs'].str.replace(r'\) +\(', '),(', regex=True)
        
        dst_df = pd.merge(dst_df, registration_df, on='identifier')
        dst_df['registrationdate'] = pd.to_datetime(dst_df['registrationdate'])
        dst_df['registrationdate'] = dst_df['registrationdate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        new_order_dst = ['identifier', 'registrationdate', 'containeridentifier', 'issued', 'dsttest', 'drugs']
        dst_df = dst_df.reindex(columns=new_order_dst)
        
        logger.info('Transform dst function completed successfully')
        return dst_df
    except Exception as e:
        logger.error(f'Transform dst function failed with error: {e}', exc_info=True)
        raise e