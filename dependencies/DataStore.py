"""
DataStore.py
~~~~~~~~

Module containing helper function for connecting to SQLite
"""
import sqlite3
from sqlite3 import Error
from miscellaneous import ApplicationConstants as constant


class DataStore(object):
    """Wrapper class for DataStore JVM object.

    :param conf: config object.
    :param log: log object.
    """

    def __init__(self, conf, log):
        # get spark app configs
        self.conf = conf
        self.logger = log

    def create_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            self.logger.error("Error # create_connection :" + e)
            raise e

        return conn

    def execute_query(self, conn, sql_statement):
        """ execute query in database
        :param conn: Connection object
        :param sql_statement: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(sql_statement)
            conn.commit()
        except Error as e:
            self.logger.error("Error # execute_query :" + e)
            raise e
        finally:
            conn.close()

    def select_run_history_by_pipeline(self, conn, etl_name):
        """
        Query pipeline_history by etl_name
        :param conn: the Connection object
        :param etl_name:
        :return: run_history
        """
        run_history = {}
        try:
            cur = conn.cursor()
            select_query = "SELECT id, etl_name, skip_execution, is_active, full_load, filter_col1_name," \
                           "filter_col1_value FROM pipeline_history WHERE is_active  = 'Y' AND etl_name= '{0}';".format(etl_name)
            cur.execute(select_query)

            rows = cur.fetchone()
            run_history = {'id': rows[0], 'etl_name': rows[1], 'skip_execution': rows[2], 'is_active': rows[3],
                       'full_load': rows[4], 'filter_col1_name': rows[5], 'filter_col1_value': rows[6]}
        except Error as e:
            self.logger.error("Error # select_run_history_by_pipeline :" + e)
            raise e
        finally:
            conn.close()

        return run_history

    def insert_invoice_pipeline_history(self, conn, max_invoice_id):
        """
        After success run make a history table entry with max invoice_id
        :param conn:
        :param max_invoice_id:
        :return:
        """
        try:
            insert_query = constant.sql_invoice_pipeline_history.format(max_invoice_id)
            self.logger.info("# insert_invoice_pipeline_history for insert_query :" + insert_query)
            self.execute_query(conn, insert_query)
        except Error as e:
            self.logger.error("Error # insert_invoice_pipeline_history :" + e)
            raise e

    def update_invoice_pipeline_history(self, conn, run_id):
        """
        After success run update history table entry with is_active = 'N'
        :param conn:
        :param run_id:
        :return:
        """
        try:
            # update_query = constant.sql_update_invoice_pipeline_history.format(run_id)
            update_query = "UPDATE pipeline_history SET is_active = 'N' where id = {0}".format(run_id)
            self.logger.info("# update_invoice_pipeline_history with update_query :" + update_query)
            self.execute_query(conn, update_query)

        except Error as e:
            self.logger.error("Error # update_invoice_pipeline_history :" + e)
            raise e
