import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_biochem(biochem_df):
    try:
        # Rename the columns to what the data dictionary says
        new_col_names = {
            'specimen': 'identifier',
            'specimen local': 'containeridentifier',
            'Date': 'collected',
            'Glucose': 'Glukoza',
            'Glucose: Diabetic Post-Meal': 'GlukozaDiabetic',
            'Creatinine': 'Kreatinini',
            'Lipase': 'Lipaza',
            'TotalWBC': 'Total WBC'
        }
        
        biochem_df.rename(columns=new_col_names, inplace=True)
        
        biochem_df['collected'] = pd.to_datetime(biochem_df['collected'])
        biochem_df['collected'] = biochem_df['collected'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        labtest_cols = [col for col in biochem_df.columns if col not in ['identifier', 'collected', 'containeridentifier']]
        
        final_biochem_df = pd.melt(biochem_df, id_vars=['identifier', 'collected', 'containeridentifier'], value_vars=labtest_cols,
                            var_name='labtests', value_name='results')
        
        final_biochem_df.sort_values(by=['identifier'], inplace=True)
        final_biochem_df.reset_index(drop=True, inplace=True)
        
        # final_biochem_df = final_biochem_df.dropna(subset=['results'])
        logger.info('Transform biochem function completed successfully')
        return final_biochem_df
    except Exception as e:
        logger.error(f'Transform biochem function failed with error: {e}', exc_info=True)
        raise e