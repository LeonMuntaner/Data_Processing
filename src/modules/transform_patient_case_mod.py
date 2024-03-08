import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_patient_case(patient_case_df):
    try:
        # Removing any leading white space from the column names. 
        patient_case_df = patient_case_df.rename(columns=lambda x: x.strip())
        patient_case_df.rename(columns={'Identifier':'identifier',
                                    'organization':'managingorganizationid',
                                    'comorbidity(Multiple)':'comorbidity'}, inplace=True)
        
        # Making all the values in these columns lower case
        patient_case_df['gender'] = patient_case_df['gender'].str.lower()
        patient_case_df['casedefinition'] = patient_case_df['casedefinition'].str.lower()
        patient_case_df['employment'] = patient_case_df['employment'].str.lower()
        
        # Transforming the values into what the data dictionary wants:
        patient_case_df['identifier'] = patient_case_df['identifier'].str.strip()
        
        patient_case_df['casedefinition'] = patient_case_df['casedefinition'].replace(
            to_replace=['Lost to follow up', 'Chronic TB'],
            value=['ltfu', 'chronic']
        )
        
        patient_case_df['localization'] = patient_case_df['localization'].replace(
            to_replace=['Pulmonary', 'Extrapulmonary', 'Pulmonary and Extrapulmonary', 'Unknown'],
            value=['pulm', 'extraPulm', 'both', 'unknown']
        )
        
        patient_case_df['comorbidity'] = patient_case_df['comorbidity'].replace(
            to_replace=['HIV', 'Diabetes', 'Hepatic diseases', 'Hepatitis B', 'Hepatitis C', 'Renal disease', 'Anemia', 'None',
                        'Not specified', 'Others', 'COVID-19', 'Post-COVID-19', 'Pneumoconiosis', 'Systemically administered glucocorticoids, cytostatics, TNF-α antagonists', 
                        'Psychiatric illness', 'Anxiety', 'Depression'],
            value=['hiv', 'diabets', 'hepaticDiseases', 'hepatitisB', 'hepatitisC', 'renalDisease', 'anemia', 'none', 'notSpecified', 
                    'others', 'COVID_19', 'Post-COVID-19', 'pneumoconiosis', 'corticosteroids', 'psychiatric', 'anxiety', 'depression']
        )
        
        patient_case_df['diagnosis'] = patient_case_df['diagnosis'].fillna('unknown')
        patient_case_df['diagnosis'] = patient_case_df['diagnosis'].replace(
            to_replace=['Unknown'],
            value=['unknown']
        )
        
        patient_case_df['dstprofile'] = patient_case_df['dstprofile'].replace(
            to_replace=['MDR non XDR', 'Mono DR', 'Poly DR', 'Sensitive', 'Pre-XDR', 'XDR', 'Negative'],
            value=['MDRnonXDR', 'monoDR', 'polyDR', 'sensitive', 'preXDR', 'XDR', 'negative']
        )
        
        patient_case_df['riskfactors'] = patient_case_df['riskfactors'].replace(
            to_replace=['Homeless', 'Ex prisoner', 'Ex Prisoner', 'Worked abroad', 'TB care worker', 'Current smoker', 'Patient drug abuse', 'Patient alcohol abuse',
                        'Documented MDR contact', 'Immigrants, refugees, internal migrants', 'Patient alcohol abuse not available/unknown', 'Patient smoking data not available/unknown',
                        'Patient smoking data not available/unkn'],
            
            value=['homeless', 'exPrisoner', 'exPrisoner', 'travelOutOfWork', 'tbFacility', 'tobacoUse', 'drugAbuse', 'alcoholism', 'ContactOfKnownMdrTbCase', 'immigrant', 'alcoholUnknown', 'smokingUnknown',
                'smokingUnknown']
        )
        
        patient_case_df['education'] = patient_case_df['education'].replace(
            to_replace=['Basic school (incl. primary)', 'Basic school', 'Complete school (a-level, gymnasium)', 'gymnasium)', 'Complete school (a-level', 'Complete school', 'College (bachelor)', 
                        'Higher (university)', 'No education'],
            value=['basicSchool', 'basicSchool', 'completeSchool', 'completeSchool', 'completeSchool', 'completeSchool', 'college', 'higher', 'noEducation']
        )
        
        patient_case_df['employment'] = patient_case_df['employment'].replace(
            to_replace=['self-employed', 'unofficially employed'],
            value=['selfEmployed', 'unofficiallyEmployed']
        )
        
        patient_case_df['totalcontacts'] = patient_case_df['totalcontacts'].replace(
            to_replace=['2 people per room', '2 people per room (6 people)', '13'],
            value=['2', '6', '>10']
        )
        
        # Replace NaN values in totalcontacts with unknown
        patient_case_df['totalcontacts'] = patient_case_df['totalcontacts'].fillna('unknown')
        
        patient_case_df['registrationdate'] = pd.to_datetime(patient_case_df['registrationdate'])
        patient_case_df['registrationdate'] = patient_case_df['registrationdate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info("Transformed patient case table")
        return patient_case_df
    except Exception as e:
        logger.error(f"Error transforming patient case table: {e}")
        raise e

def transform_condition(condition_df):
    try:
        condition_df['comments'] = condition_df['comments'].astype(str)
        condition_df['dstnegativereason'] = condition_df['dstnegativereason'].astype(str)
        condition_df['riskfactors'] = condition_df['riskfactors'].astype(str)
        
        condition_df['weight'] = condition_df['weight'].astype(str)
        condition_df['height'] = condition_df['height'].astype(str)
        condition_df['weight'] = condition_df['weight'].str.replace(' kg', '')
        
        condition_df['height'] = condition_df['height'].str.replace(' m', '')
        condition_df['height'] = condition_df['height'].str.replace('m', '')
        condition_df['weight'] = condition_df['weight'].str.replace(' m', '')
        condition_df['height'] = condition_df['height'].str.replace(' kg', '')
        
        condition_df['totalcontacts'] = condition_df['totalcontacts'].astype(str)
        condition_df['totalchildren'] = condition_df['totalchildren'].astype(str)
        
        logger.info("Transformed condition table")
        return condition_df
    except Exception as e:
        logger.error(f"Error transforming condition table: {e}")
        raise e