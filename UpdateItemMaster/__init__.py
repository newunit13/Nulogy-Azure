import datetime
import logging
import azure.functions as func
import pandas as pd
import numpy as np
from utils import nulogy, sql


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Getting Item Master data from Nulogy')

    report_code = 'item_master'
    columns = ['account_name','accounting_unit_of_measure','alternate_code_1','alternate_code_2','auto_backflush',
               'auto_quarantine_on_production','auto_quarantine_on_production_status_override','auto_quarantine_on_receipt',
               'base_unit_of_measure','case_unit_of_measure','code','cost_per_unit','country_of_origin','created_at','custom_item_field_1',
               'custom_item_field_2','custom_item_field_23','custom_item_field_24','custom_item_field_25','custom_item_field_3',
               'custom_item_field_4','custom_item_field_5','customer','customer_product_code','default_unit_of_measure',
               'description','expiry_date_format','expiry_date_policy','expiry_date_rule','export_to_accounting',
               'external_identifier','freight_class','full_pallet_unit_of_measure','inactive','include_in_jit_line_replenishment',
               'include_in_material_ordering','include_in_picking','is_finished_good','is_subcomponent','item_category',
               'item_class','item_family','item_type','lead_time_days','lead_time_type','line_type_preference_primary',
               'line_type_preference_secondary','lot_code_policy','lot_code_rule','minimum_order_quantity','nmfc_code',
               'order_increment','performance','personnel','pick_strategy','pick_strategy_source','preferred_line_type',
               'production_rate_depends_on_number_of_people','quick_consume','receiving_unit_of_measure',
               'reconciliation_physical_difference_percentage_limit','reconciliation_unit_of_measure','reject_rate',
               'reorder_strategy','require_physical_count_during_reconciliation','safety_stock','safety_stock_unit_of_measure',
               'service_category','setup_time','shelf_life_label','standard_units_per_hour','stop_ship_limit','teardown_time',
               'track_lot_code_by','track_pallets','unit_purchase_price','updated_at','uuid','vendor','weight_per_case','weight_per_pallet']

    report = nulogy.get_report(report_code, columns)

    logging.info('Inserting Item Master data into SQL')
    sql.execute(f"DELETE FROM factItemMaster")

    field_names = next(report)
    nulogy_df = pd.DataFrame.from_records(report, columns=field_names)
    nulogy_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    nulogy_df.to_sql('factItemMaster', sql.engine, if_exists='replace', index=False, chunksize=1000)


    #for row in report:
    #    row = {k: v for k, v in zip(headers, row)}
    #    sql.insert(table='factItemMaster', record=row)


    logging.info('UpdateItemMaster function ran at %s', utc_timestamp)
