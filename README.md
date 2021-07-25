# invoice_pipeline

This repository will be used for managing all the code related to pipeline for Personio invoice data.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- DataStore.py
 |   |-- logging.py
 |   |-- spark.py
 |-- helpers/
 |   |-- APIClient.py
 |   |-- GenericHelper.py
 |-- jobs/
 |   |-- invoice_etl_job.py
 |-- miscellaneous  /
 |   |-- ApplicationConstants.py
 |-- script  /
 |   |-- lambda
         |-- pipeline_lambda.py
 |-- resources  /
     |-- invoice /
         |-- data /
 |-- tests/
 |   |-- test_data/
 |   |-- | -- employees/
 |   |-- | -- employees_report/
 |   |-- test_invoice_etl_job.py.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```

# Synopsis

    invoices data available in Chargebee, 
    We want you to write clean, packaged and well documented code which reads the following
    columns from this table and saves it in a sqlite database:
    ● customer_id (unique ID for every customer)
    ● date (date of the invoice)
    ● discounts (list with detailed information about discounts)
    ● due_date (date of the payment deadline of an invoice)
    ● id (unique ID for every invoice, primary key)
    ● line_items (list of products and add ons which are part of the invoice, with
    information about their price and discounts)
    ● status (current status of the invoice)
    ● subscription_id (unique ID for every subscription)
    Please format these columns so that they are ready to use from Analysts (e.g. use a proper
    timezone for dates).



# Requirements

    python 3.6 or higher
    Spark 2.3.3
    SQLite version 3.36.0
    *AWS Lambda
    *AWS EMR

    * If we want to run the job on AWS EMR Using AWS Lambda.
    
# SQLite schema & table Creation

    sqlite3 personio.db
    ATTACH DATABASE "<db_file_path>" AS personio;
    
    CREATE TABLE IF NOT EXISTS pipeline_history (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       etl_name CHAR(60) NOT NULL,
       skip_execution CHAR(1) NOT NULL,
       is_active CHAR(1) NOT NULL,
       full_load CHAR(1) NOT NULL,
       run_date timestamp,
       filter_col1_name VARCHAR(100),
       filter_col1_value  VARCHAR(1000),
       filter_col2_na   me VARCHAR(100),
       filter_col2_value VARCHAR(1000)
       );

    INSERT INTO pipeline_history (etl_name , skip_execution, is_active, full_load, run_date, filter_col1_name)
    VALUES ('invoice-data', 'N', 'Y', 'Y', current_timestamp, 'invoice_id');
   
# Run
    
    1. Change your configs/etl_config.json entries according to your 
       configuration.
    
    2. Configure script/lambda/pipeline_lambda.py with propper
        
        "spark.driver.cores": "2",
        "spark.executor.cores": "2",
        "spark.driver.memory": "13G",
        "spark.executor.memory": "13G",
        "spark.driver.memoryOverhead": "1460M",
        "spark.executor.memoryOverhead": "1460M",
        "spark.executor.instances": "10",
        "spark.default.parallelism": "50"
        
    3. Then run jobs/invoice_etl_job.py in cluster mode.
