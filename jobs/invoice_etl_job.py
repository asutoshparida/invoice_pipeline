from pyspark.sql import Row
from dependencies.spark import start_spark
from helpers import APIClient
from dependencies import DataStore as db
from miscellaneous import ApplicationConstants as constant
from helpers import GenericHelper as helper


def main():
    """Main ETL script definition.

    :return: None
    """
    work_env = "DEV"
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='personio-invoice-pipeline', jar_packages=['org.xerial:sqlite-jdbc:3.34.0'],
        files=['configs/etl_config.json'], environment=work_env)

    # log that main ETL job is starting
    log.info('etl_pipeline is up-and-running')
    final_invoices_df = None
    final_items_df = None
    total_flat_invoices_df = None

    ''' execute API call to get incremental invoice data '''
    api_client = APIClient.APIClient(config, log)
    api_client.export_api_data_json()

    ''' Connect to SqlLite & get pipeline run history data '''
    sqlite_bd = db.DataStore(config, log)
    conn = sqlite_bd.create_connection(config['db_file'])
    run_history = sqlite_bd.select_run_history_by_pipeline(conn, 'invoice-data')

    run_id = run_history['id']
    full_load = run_history['full_load'] if run_history is not None else 'Y'
    skip_execution = run_history['skip_execution'] if run_history is not None else 'Y'
    filter_col1_name = run_history['filter_col1_name'] if run_history is not None else 'invoice_id'
    filter_col1_value = int(run_history['filter_col1_value']) if run_history['filter_col1_value'] is not None and run_history['filter_col1_value'] != 'null' else 0
    log.info('pipeline running with : run_id :' + str(run_id) + "# full_load :" + full_load + "# skip_execution:" + skip_execution + "# filter_col1_name:" + filter_col1_name + "# filter_col1_value :" + str(filter_col1_value))
    if skip_execution == 'N':
        generic_helper = helper.GenericHelper(config, log)

        if full_load == 'Y':
            '''
                If it's a full load then create the tables in sqlite
            '''
            conn = sqlite_bd.create_connection(config['db_file'])
            sqlite_bd.execute_query(conn, constant.sql_create_invoices_table)
            # Parse JSOn file to extract data
            total_flat_invoices_df, final_invoices_df, final_items_df = generic_helper.load_invoices_data(spark, config, None)
        elif full_load == 'N':
            '''
                If it's a delta load then get the delta data
            '''
            total_flat_invoices_df, final_invoices_df, final_items_df = generic_helper.load_invoices_data(spark, config, filter_col1_value)

        if final_invoices_df is not None and final_items_df is not None and not final_invoices_df.rdd.isEmpty():
            ''' 
            If final_invoices_df has data then 
            join invoice & line_items data set and flush to sqlite
            '''
            merged_data_frame = generic_helper.join_invoice_data(final_invoices_df, final_items_df)
            # flush invoice data to stage table in DB
            if full_load == 'Y':
                generic_helper.push_data_frame_sqlite(merged_data_frame, table_name='invoice_data',
                                                      write_mode='overwrite')
            elif full_load == 'N':
                generic_helper.push_data_frame_sqlite(merged_data_frame, table_name='invoice_data',
                                                      write_mode='append')

            merged_data_frame.show()

            '''
            retrieve max_invoice_id for that run then makes a new pipeline_history 
            and update the current pipeline_history entry is_active to 'N'
            '''
            max_invoice_id = generic_helper.select_max_invoice_id(final_invoices_df)
            conn = sqlite_bd.create_connection(config['db_file'])
            sqlite_bd.update_invoice_pipeline_history(conn, run_id)
            conn = sqlite_bd.create_connection(config['db_file'])
            sqlite_bd.insert_invoice_pipeline_history(conn, max_invoice_id)

            '''
            After successful run flush total_flat_invoices_df as parquet to data lake[S3]
            '''
            generic_helper.write_data_frame_as_parquet(total_flat_invoices_df, config['parquet_file_location'])

    # log the success and terminate Spark application
    log.info('etl_pipeline is finished')
    spark.stop()
    return None


def create_test_data(spark, config, work_env):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    if work_env.upper() == 'DEV':
        spark.conf.set("spark.sql.session.timeZone", "Singapore")
    elif work_env.upper() == 'PROD':
        spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")

    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    # df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    # (df_tf
    #  .coalesce(1)
    #  .write
    #  .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
