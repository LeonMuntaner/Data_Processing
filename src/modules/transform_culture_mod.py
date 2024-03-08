import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_culture(culture_df, registration_df):
    try:
        culture_df.rename(columns={'patient_local_Identifier':'identifier', 
                            'spec_local_identifier':'containeridentifier', 
                            'collected_date':'issued',
                            'result':'value'}, inplace=True)
        
        # For some reason this column name did not want to change unless I did it like this. 
        culture_df.rename(columns={culture_df.columns[3]: "culturetype"}, inplace = True)
        
        culture_df['value'] = culture_df['value'].replace(
            to_replace=['1 to 19', '20 to 100', '20 to100', '100 to 200', 'More than 200', 'Positive', 'Negative', 'Unknown result', 'Study in progress', 'Not done', 
                        'Nonspecific microflora', 'MOTT', 'MTBc Positive', 'MTBc Negative', 'Contamited', 'No Growth', 'MTBc Positive & Contamited'],
            value=['singleColony', '1+', '1+', '2+', '3+', 'positive', 'negative', 'unknownData', 'unfinishedResult', 'notDone', 'contamination', 'mott', 'positive',
                    'negative', 'contamination', 'negative', 'contamination']
        )
        
        culture_df['value'] = culture_df['value'].fillna('unknownData')
        
        culture_df['culturetype'] = culture_df['culturetype'].replace(
            to_replace=['Liquid', 'Solid', 'Not specified'],
            value=['liquid', 'solid', 'notSpecified']
        )
        
        culture_df['issued'] = pd.to_datetime(culture_df['issued'])
        culture_df['issued'] = culture_df['issued'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        culture_df = pd.merge(culture_df, registration_df, on='identifier')
        
        culture_df['registrationdate'] = pd.to_datetime(culture_df['registrationdate'])
        culture_df['registrationdate'] = culture_df['registrationdate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        new_order_culture = ['identifier', 'registrationdate', 'containeridentifier', 'issued', 'value', 'culturetype']
        culture_df = culture_df.reindex(columns=new_order_culture)
        
        logger.info('Transform culture function completed successfully')
        return culture_df
    except Exception as e:
        logger.error(f'Transform culture function failed with error: {e}', exc_info=True)
        raise e