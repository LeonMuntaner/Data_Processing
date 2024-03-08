import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_specimen(specimen_df, registration_df):
    try:
        specimen_df.rename(columns={'patient_local_identifier':'identifier', 
                            'spec_local_identifier':'containeridentifier', 
                            'collected_date':'collected', 
                            'collection_bodysite':'bodysite'}, inplace=True)
        
        # SAB (aspirat bronhic) and Saliva are not values that the TB Portals data dictionary has replacements for.
        # After talking with Nik he agreed wit mapping SAB (aspirat bronhic) and Saliva to 'other'
        specimen_df['bodysite'] = specimen_df['bodysite'].replace(
            to_replace=['Biopsy', 'Sputum', 'Other', 'Surgery-caseous masses', 'Surgery-cavity internal wall', 'Surgery-cavity external wall', 
                        'Surgery-infectious granuloma', 'Surgery-healthy tissue', 'Bronchial lavage', 'Ascitic fluid', 'Blood', 'Urine', 'Pleural fluid',
                        'Cerebrospinal Fluid', 'Paraffin Embedded Tissue', 'SAB (aspirat bronhic)', 'Saliva', 'Tracheal aspirate', 'Sputum, Surgery-infectious granuloma'],
            
            value=['biopsy', 'sputum', 'other', 'surgeryCaseousMasses', 'surgeryCavityInternalWall', 'surgeryCavityExternalWall', 'surgeryModulu',
                    'surgeryHealthy tissue', 'bronchialLavage', 'asciticFluid', 'blood', 'urine', 'pleuralFluid', 'cerebrospinalFluid', 'paraffinEmbeddedTissue', 'other', 
                    'other', 'other', 'other']
        )
        
        specimen_df['collected'] = pd.to_datetime(specimen_df['collected'])
        specimen_df['collected'] = specimen_df['collected'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        specimen_df = pd.merge(specimen_df, registration_df, on='identifier')
        
        specimen_df['registrationdate'] = pd.to_datetime(specimen_df['registrationdate'])
        specimen_df['registrationdate'] = specimen_df['registrationdate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        new_order_specimen = ['identifier', 'registrationdate', 'containeridentifier', 'collected', 'bodysite']
        
        specimen_df = specimen_df.reindex(columns=new_order_specimen)
        
        logger.info('Specimen data transformed')
        return specimen_df
    except Exception as e:
        logger.error(f'Error in transform_specimen: {e}')
        raise e