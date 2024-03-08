# TB Portals Country Data Bulk-Uploads

## Project Overview
This ETL project is designed to process country-specific data residing in AWS S3 buckets for our Case Browser application. The data is extracted from the S3 buckets, transformed into a usable format, and loaded into a PostgreSQL database. The data is then used to populate the Case Browser application.

---
### Analysis Process:

* Perform EDA and check what are all the datapoints we have to upload. As in, do we have to upload patient, specimen, culture and so on?

* Within the datapoints what are the distinct values for each attribute (e.g. Say specimen has bodysite column, then what are the distinct values of that column?)

* Map the values directly to the values that exist in [Case Browser Data Dictionary](https://data.tbportals.niaid.nih.gov/dictionary)

* If not then we need to apply certain transformations so that the values meet the Data Dictionary structure.
---
### Source Code Location:
Source code can be found here: `src/` 

* All the necessary functions are stored in the following file: `src/modules/`
    * These functions handle most of the data transformations for the necessary tables. 
---
### S3 Bucket Information:
* Bucket Name: `bulk-country-data.tbportals.niaiddev.net`
* Folder Structure: 
    * First Level: `COUNTRIES`
    * Second Level: Country names
    * Third Level: Datestamp received (format: YYYYMMDD)
    * Fourth Level: [`raw`, `interm`, `processed`]
    
    Example: 
    ```
    /COUNTRIES
    |
    |____
    |    MEXICO/
    |    |
    |    |____2023-01-01/
    |         |
    |         |____raw/
    |         |____interim/
    |         |____processed/
    |
    |____GEORGIA/
    |
    |
    ```
---
### Replicating the ETL Process:
1. When a new country data batch is received, create a new folder in the S3 bucket named `YYYYMMDD` in the country folder, and inside it create folders named `raw`, `interim`, and `processed`.
2. Place the source data in the `raw` folder in S3.
3. *Create a branch* for the country data release, named `{COUNTRY}_{YYYYMMDD}`
4. Clone the branch to your compute environment.
5. Manipulate the data using a `data_main.ipynb` notebook.
---
### Notebooks:
1. Data Manipulation Notebooks
    * `data_main.ipynb` - This notebook contains the code for transforming the data into the necessary format for the Case Browser application. 
    * `Data_main_legacy.ipynb` - **This notebook contains the code for transforming the Georgia data into the necessary format for the Case Browser application.**
    
    **Note:** 
    * Notebooks are parameterized with globals such as `country_name`, `submission_date`.
    * Before starting, configure the notebooks to specify the S3 bucket, folder, and file name.
    * Notebook will connect to S3, download the file programmatically to a temporary location, perform the necessary transformations, and upload the transformed data back to S3 in the `processed` folder.
    * Commit the code back to the GitHub branch once the work is complete.

2. Loading Data into Database Notebook
    * `data_load.ipynb` - This notebook takes data from the 'processed' folder in the S3 bucket and loads it into the PostgreSQL database.
    
    **Note:** 
    * Uses read-only access to S3
    * At the end, deletes the data from local storage to separate compute and storage
---
### Secrets Management:
*  Notebooks refrence secrets from the AWS Secrets Manager
---
### Development Environment:
For development:
1. Spin up a dev instance on AWS
2. Clone the repository to the dev instance
3. Work via SSH in VS Code on the dev instance
4. Ensure the dev instance has IAM policies attached for S3 access and database login.

**Note:** All access is through IAM roles and policies. You should not be using Admin accounts from local environments.