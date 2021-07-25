"""
ApplicationConstants.py
"""

sql_create_invoices_table = """CREATE TABLE IF NOT EXISTS invoice_data (
   invoice_id integer,
   invoice_date text NOT NULL,
   invoice_due_date text NOT NULL,
   total integer, 
   amount_paid integer,
   amount_due integer,
   invoice_paid_date text,
   tax integer,
   customer_id text NOT NULL,
   subscription_id text,
   status text,
   currency_code text,
   id integer NOT NULL,
   date_from text NOT NULL,
   date_to text NOT NULL,
   discount_amount integer, 
   amount integer,
   item_level_discount_amount integer,
   quantity integer,
   unit_amount integer,
   is_taxed_item text
   );    """

sql_invoice_pipeline_history = """INSERT INTO pipeline_history (etl_name , skip_execution, 
    is_active, full_load, run_date, filter_col1_name, filter_col1_value) 
    VALUES ('invoice-data', 'N', 'Y', 'N', current_timestamp, 'invoice_id', {0})"""

sql_update_invoice_pipeline_history = """UPDATE pipeline_history SET is_active = 'N' where id = {0}"""
