"""
APIClient.py
~~~~~~~~

Module containing generic helpers for our pipeline
"""
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, lit


class GenericHelper(object):
    """class containing all generic functions

    :param conf: config object.
    :param log: log object.
    """

    def __init__(self, conf, log):
        # get log app configs
        self.conf = conf
        self.logger = log

    def load_invoices_data(self, spark, config, invoice_id):
        """read a json file and prepare 3 separate data frame

        final_invoices_df                                       final_items_df
        -----------------                                       ---------------

            root                                                root
         |-- id: string (nullable = true)                        |-- invoice_id: string (nullable = true)
         |-- invoice_date: string (nullable = true)              |-- amount: long (nullable = true)
         |-- invoice_due_date: string (nullable = true)          |-- customer_id: string (nullable = true)
         |-- customer_id: string (nullable = true)               |-- date_from: long (nullable = true)
         |-- subscription_id: string (nullable = true)           |-- date_to: long (nullable = true)
         |-- status: string (nullable = true)                    |-- discount_amount: long (nullable = true)
         |-- currency_code: string (nullable = true)             |-- id: string (nullable = true)
         |-- invoice_paid_date: string (nullable = true)         |-- is_taxed: boolean (nullable = true)
         |-- amount_due: long (nullable = true)                  |-- item_level_discount_amount: long (nullable = true)
         |-- amount_paid: long (nullable = true)                 |-- quantity: long (nullable = true)
         |-- tax: long (nullable = true)                         |-- subscription_id: string (nullable = true)
                                                                 |-- unit_amount: long (nullable = true)


        :return: DataFrame
        """
        report_file_path = config['report_path']

        # use multiline = true to read multi line JSON file
        raw_invoice_df = spark.read.option("multiLine", "true").json(report_file_path)

        # Explode Array to Structure & filter delta data
        tmp_flat_invoices_df = raw_invoice_df.withColumn('flat_invoice', F.explode(F.col('list'))).drop('list')
        if invoice_id is not None and invoice_id > 0:
            flat_invoices_df = tmp_flat_invoices_df.select("*").where(tmp_flat_invoices_df['flat_invoice.invoice.id'] > invoice_id)
        else:
            flat_invoices_df = tmp_flat_invoices_df
        flat_invoices_df.printSchema()

        # Flatten the rest columns
        final_invoices_df = flat_invoices_df.select("flat_invoice.invoice.id",
                                                    F.from_unixtime("flat_invoice.invoice.date",
                                                                    "yyyy-MM-dd HH:mm:ss.SSS").alias("invoice_date"),
                                                    F.from_unixtime("flat_invoice.invoice.due_date",
                                                                    "yyyy-MM-dd HH:mm:ss.SSS").alias(
                                                        "invoice_due_date"),
                                                    "flat_invoice.invoice.total",
                                                    "flat_invoice.invoice.amount_paid",
                                                    "flat_invoice.invoice.amount_due",
                                                    F.from_unixtime("flat_invoice.invoice.paid_at",
                                                                    "yyyy-MM-dd HH:mm:ss.SSS").alias(
                                                        "invoice_paid_date"),
                                                    "flat_invoice.invoice.tax",
                                                    "flat_invoice.invoice.customer_id",
                                                    "flat_invoice.invoice.subscription_id",
                                                    "flat_invoice.invoice.status",
                                                    "flat_invoice.invoice.currency_code")

        final_items_df = flat_invoices_df.select(col("flat_invoice.invoice.id").alias("invoice_id"),
                                                 col("flat_invoice.invoice.line_items")). \
            withColumn('flat_items', F.explode(F.col('line_items'))).drop('line_items'). \
            select("invoice_id", "flat_items.amount",
                   F.from_unixtime(
                       "flat_items.date_from",
                       "yyyy-MM-dd HH:mm:ss.SSS").alias(
                       "date_from"),
                   F.from_unixtime(
                       "flat_items.date_to",
                       "yyyy-MM-dd HH:mm:ss.SSS").alias(
                       "date_to"), "flat_items.discount_amount", "flat_items.item_level_discount_amount",
                   "flat_items.customer_id", "flat_items.subscription_id", "flat_items.id", "flat_items.quantity",
                   "flat_items.unit_amount",
                   "flat_items.is_taxed").withColumn("is_taxed_item", col("is_taxed").cast("string")).drop("is_taxed")

        final_invoices_df.printSchema()
        final_items_df.printSchema()

        return flat_invoices_df, final_invoices_df, final_items_df

    def push_data_frame_sqlite(self, data_frame, table_name, write_mode):
        """
        Takes a DataFrame & bulk insert to SQLite DB
        :param data_frame:
        :param table_name:
        :param write_mode:
        :return:
        """
        self.logger.info("Exporting data to database for table :" + table_name + ", with write mode :" + write_mode)
        try:
            data_frame.write.format('jdbc') \
                .options(driver='org.sqlite.JDBC', dbtable=table_name,
                         url='jdbc:sqlite:' + self.conf['db_file']).mode(write_mode).save()
        except Exception as e:
            self.logger.error("Error # pushDataFrameToSQlite :" + e)
            raise e

    def join_invoice_data(self, invoice_data_frame, list_data_frame):
        """
            Takes two DataFrame of invoice & line_items then join them on invoice_id, customer_id, subscription_id columns
            :param invoice_data_frame:
            :param list_data_frame:
            :return data_frame:
        """
        merged_data_frame = invoice_data_frame.join(list_data_frame,
                                        (invoice_data_frame.id == list_data_frame.invoice_id) &
                                        (invoice_data_frame.customer_id == list_data_frame.customer_id) &
                                        (invoice_data_frame.subscription_id == list_data_frame.subscription_id))\
                                        .select(invoice_data_frame["invoice_date"],
                                        invoice_data_frame["invoice_due_date"],
                                        invoice_data_frame["total"],
                                        invoice_data_frame["amount_paid"],
                                        invoice_data_frame["amount_due"],
                                        invoice_data_frame["invoice_paid_date"],
                                        invoice_data_frame["tax"],
                                        invoice_data_frame["customer_id"],
                                        invoice_data_frame["subscription_id"],
                                        invoice_data_frame["status"],
                                        invoice_data_frame["currency_code"],
                                        list_data_frame["invoice_id"],
                                        list_data_frame["amount"],
                                        list_data_frame["date_from"],
                                        list_data_frame["date_to"],
                                        list_data_frame["discount_amount"],
                                        list_data_frame["item_level_discount_amount"],
                                        list_data_frame["id"],
                                        list_data_frame["quantity"],
                                        list_data_frame["unit_amount"],
                                        list_data_frame["is_taxed_item"])

        merged_data_frame.printSchema()
        return merged_data_frame

    def select_max_invoice_id(self, invoice_data_frame):
        """
        extract maximum invoice_id for that run
        :param invoice_data_frame:
        :return: max_invoice_id
        """
        max_invoice_id = invoice_data_frame.agg({"id": "max"}).collect()[0][0]
        self.logger.info("# select_max_invoice_id :" + str(max_invoice_id))

        return max_invoice_id

    def write_data_frame_as_parquet(self, invoice_data_frame, location):
        """
        write invoice_data_frame as parquet for that run
        :param invoice_data_frame:
        :param location:
        :return:
        """
        # invoice_data_frame.write.parquet(location)
        self.logger.info("# invoice_data_frame written to :" + str(location))

    def transform_data(df, steps_per_floor_):
        """Transform original dataset.

        :param df: Input DataFrame.
        :param steps_per_floor_: The number of steps per-floor at 43 Tanner
            Street.
        :return: Transformed DataFrame.
        """
        df_transformed = (
            df
                .select(
                col('id'),
                concat_ws(
                    ' ',
                    col('first_name'),
                    col('second_name')).alias('name'),
                (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

        return df_transformed
