import datetime
import logging
import pytz
import asyncio
from utils import nulogy, sql
import azure.functions as func
import pandas as pd


def timestamp() -> str:
    return datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M')

async def process_ship_orders (days: int=28) -> None:
    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d 00:00")
    to_threshold = timestamp.strftime("%Y-%m-%d 23:59")

    report_code = 'ship_order'
    columns = ['actual_unit_quantity', 'added_unit_quantity', 'carrier', 'carrier_code', 'carrier_type', 
               'detailed_purchase_order_number', 'expected_ship_at', 'expected_unit_quantity', 'full_pallet_quantity', 
               'full_pallet_weight', 'full_pallets_unit_of_measure', 'item_alternate_code_1', 'item_alternate_code_2', 
               'item_category_name', 'item_class', 'item_code', 'item_customer', 'item_description', 'item_family_name', 
               'item_type_name', 'number_of_shipments', 'ship_order_code', 'ship_order_created_at', 'ship_order_customer', 
               'ship_order_notes', 'ship_order_reference', 'ship_to', 'ship_to_facility_number', 'shipped', 'shipped_at', 
               'site_name', 'unit_of_measure']
    filters = [{'column': 'ship_order_created_at', 'operator': 'between', 'from_threshold': from_threshold, 'to_threshold': to_threshold}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    # remove old records
    sql.execute(f"DELETE FROM factShipOrder WHERE [Ship Order created at] BETWEEN '{from_threshold}' AND '{to_threshold}'")

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert(table='factShipOrder', record=row)

async def process_shipments (days: int=28) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d 00:00")
    to_threshold = timestamp.strftime("%Y-%m-%d 23:59")

    report_code = 'shipment_item'
    columns = ['actual_arrival_at', 'actual_delivery_at', 'actual_ship_at', 'base_quantity', 
               'base_unit_of_measure', 'bill_of_lading_number', 'bill_to', 'carrier_code', 
               'carrier_name', 'carrier_type', 'case_quantity', 'cases_unit_of_measure', 
               'created_at', 'default_quantity', 'default_unit_of_measure', 'dock_appointment_id', 
               'estimated_delivery_at', 'expected_arrival_at', 'expiry_date', 'freight_charge_amount', 
               'freight_charge_terms', 'full_pallet_quantity', 'full_pallets_unit_of_measure', 
               'internal_notes', 'inventory_category', 'inventory_status_name', 'invoice', 
               'item_alternate_code_1', 'item_alternate_code_2', 'item_category_name', 'item_class', 
               'item_code', 'item_customer_name', 'item_description', 'item_family_name', 
               'item_material_cost_per_unit', 'item_type_name', 'lot_code', 'master_bill_of_lading_number', 
               'pallet_number', 'produced_by', 'project_po_line_item_number', 'project_purchase_order_number', 
               'seal_number', 'ship_from', 'ship_order_carrier_name', 'ship_order_code', 'ship_order_customer_name', 
               'ship_order_date_at', 'ship_order_expected_ship_at', 'ship_order_id', 'ship_order_notes', 
               'ship_order_reference_number', 'ship_order_ship_to_address', 'ship_order_shipped', 'ship_to', 
               'ship_to_facility_number', 'shipment_customer_code', 'shipment_customer_name', 'shipment_expected_ship_at', 
               'shipment_invoiced', 'shipment_item_purchase_order_number', 'shipment_notes', 'site_name', 
               'tracking_number', 'trailer_number']
    filters = [{'column': 'created_at', 'operator': 'between', 'from_threshold': from_threshold, 'to_threshold': to_threshold}]
               
    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    # remove old records
    sql.execute(f"DELETE FROM factShipment WHERE [Created At] BETWEEN '{from_threshold}' AND '{to_threshold}'")

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert(table='factShipment', record=row)

async def process_receipts (days: int=28) -> None:
    
    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
    to_threshold = timestamp.strftime("%Y-%m-%d %H:%M")

    report_code = 'receipt_item'
    columns = ['base_quantity', 'base_unit_of_measure', 'bill_of_lading', 'case_quantity', 'case_unit_of_measure', 
               'default_quantity', 'default_unit_of_measure', 'expiry_date', 'full_pallet_quantity', 'full_pallet_unit_of_measure', 
               'internal_notes', 'inventory_category', 'inventory_status', 'item_alternate_code_1', 'item_alternate_code_2', 
               'item_category_name', 'item_class', 'item_code', 'item_customer_name', 'item_description', 'item_family_name', 
               'item_shelf_life', 'item_type_name', 'location_name', 'lot_code', 'original_base_quantity', 'original_base_unit_of_measure', 
               'original_default_quantity', 'original_default_unit_of_measure', 'original_item_code', 'packing_slip', 'pallet_number', 
               'planned_receipt_expected_receive_at', 'planned_receipt_id', 'project_code', 'receipt_carrier_code', 'receipt_carrier_name', 
               'receipt_carrier_type', 'receipt_customer_name', 'receipt_item_notes', 'receipt_reference_1', 'receipt_reference_2', 
               'receipt_status', 'receive_order', 'receive_order_carrier_name', 'receive_order_code', 'receive_order_customer_name', 
               'receive_order_expected_quantity', 'receive_order_expected_unit_of_measure', 'receive_order_item_expected_delivery_at', 
               'receive_order_item_unit_purchase_price', 'receive_order_received', 'receive_order_reference', 'received_at', 'received_by', 
               'receiving_quantity', 'receiving_unit_of_measure', 'site_name', 'trailer_or_container', 'vendor_name']
    filters = [{'column': 'received_at', 'operator': 'between', 'from_threshold': from_threshold,
                                                                    'to_threshold': to_threshold}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    # remove old records
    sql.execute(f"DELETE FROM factReceipt WHERE [Received at] BETWEEN '{from_threshold}' AND '{to_threshold}'")

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert(table='factReceipt', record=row)

async def process_moves (days: int=2) -> None:
    
    logging.info('==Processing moves')

    start_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (start_time - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
    to_threshold = start_time.strftime("%Y-%m-%d %H:%M")


    logging.info(f"Getting Azure data... {timestamp()}")
    query = f"""
    SELECT *
    FROM factMove
    WHERE [Time completed] BETWEEN '{from_threshold}' AND '{to_threshold}'"""

    azure_df =  pd.read_sql(query, sql.engine)


    logging.info(f"Getting Nulogy data... {timestamp()}")
    report_code = 'move_transaction'
    columns = ['assigned_to', 'from_location', 'from_pallet_number', 'to_location', 'to_pallet_number', 'time_completed_at', 
               'item_code', 'base_quantity', 'base_unit_of_measure']
    filters = [{'column': 'time_completed_at', 'operator': 'between', 'from_threshold': from_threshold,
                                                                      'to_threshold': to_threshold}]
    
    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)


    logging.info(f"Getting new data... {timestamp()}")
    field_names = next(report)
    nulogy_df = pd.DataFrame.from_records(report, columns=field_names)

    # Converting nulogy_df dtypes to match azure_df
    convert_dict = {column: dtype for column, dtype in zip(azure_df.columns, azure_df.dtypes)}
    nulogy_df = nulogy_df.astype(convert_dict)

    # Get new unique records
    unique_df = pd.concat([nulogy_df, azure_df]).drop_duplicates(keep=False)

    logging.info(f"Writing {len(unique_df)} new records to Azure... {timestamp()}")
    unique_df.to_sql('factMove', sql.engine, if_exists='append', index=False, chunksize=1000)

    logging.info(f"==Finished processing moves... {timestamp()}")

async def process_picks(days: int=7) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    report_code = 'picked_inventory'
    columns = ['date_picked_at', 'drop_off_location', 'expiry_date', 'item_alternate_code_1', 'item_alternate_code_2', 
               'item_category_name', 'item_class_name', 'item_code', 'item_customer', 'item_description', 'item_family_name', 
               'item_type_name', 'lot_code', 'pallet_number', 'pick_up_location', 'picked_by', 'project_code', 'project_id', 
               'project_reference_1', 'project_reference_2', 'project_reference_3', 'ship_order_id', 'ship_order_reference_number', 
               'site_name', 'status', 'unit_picks_unit_of_measure', 'unit_quantity']
    filters = [{'column': 'date_picked_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"),
                                                                      'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    headers.append('Timestamp')
    for row in report:
        row.append(timestamp.strftime("%Y-%m-%d %H:%M"))
        row = {k: v for k, v in zip(headers, row)}
        sql.insert_or_update(table='factPickedInventory', key=headers[:-1], record=row)

async def process_invoice_report (days: int=28) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d 00:00")
    to_threshold = timestamp.strftime("%Y-%m-%d 23:59")

    report_code = 'invoice'
    columns = ['alternate_code_1', 'alternate_code_2', 'bill_to', 'charge_per_unit', 'customer_code', 'customer_name',
               'customer_notes', 'internal_notes', 'invoice_status', 'invoiced_at', 'item_category_name', 'item_code', 
               'item_description', 'item_family_name', 'item_type_name', 'job_id', 'notes', 'paid_at', 'payment_due_on', 
               'po_line_item_number', 'project_code', 'project_id', 'project_reference_1', 'project_reference_2', 
               'project_reference_3', 'purchase_order_number', 'reference_1', 'reference_2', 'shipment_id', 'site_name', 
               'terms', 'total_charge', 'unit_of_measure', 'unit_quantity']
    filters = [{'column': 'invoiced_at', 'operator': 'between', 'from_threshold': from_threshold,
                                                                'to_threshold': to_threshold}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    report = [row for row in report]

    sql.execute(f"DELETE FROM factInvoice WHERE [Invoice date] BETWEEN '{from_threshold}' AND '{to_threshold}'")

    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert(table='factInvoice', record=row)

async def process_job_profitability_report (days: int=28) -> None:
    
    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d 00:00")
    to_threshold = timestamp.strftime("%Y-%m-%d 23:59")

    # Job Profitability Report
    report_code = 'job_profitability'
    columns = ['actual_cost_of_labor_and_materials', 'actual_cost_of_labor_and_materials_per_unit', 'actual_gross_margin', 
    'actual_gross_profit', 'actual_job_end_at', 'actual_job_start_at', 'actual_labor_cost', 'actual_labor_cost_per_person_hour', 
    'actual_labor_cost_per_unit', 'actual_labor_margin', 'actual_labor_profit', 'actual_machine_cost', 'actual_machine_cost_per_unit', 
    'actual_materials_cost', 'actual_materials_cost_per_unit', 'actual_person_hours', 'actual_person_hours_payable', 
    'actual_person_hours_per_unit', 'actual_person_hours_productive', 'actual_total_cost', 'actual_total_cost_per_unit', 
    'actual_total_margin', 'actual_total_profit', 'actual_units_per_person_hour', 'availability', 'customer_name', 'duration', 
    'estimate_name', 'expected_cost_of_labor_and_materials', 'expected_cost_of_labor_and_materials_per_unit', 'expected_gross_margin', 
    'expected_gross_profit', 'expected_labor_cost', 'expected_labor_cost_per_unit', 'expected_labor_margin', 'expected_labor_profit', 
    'expected_materials_cost', 'expected_materials_cost_per_unit', 'expected_total_charge', 'expected_total_charge_per_unit', 
    'expected_total_cost', 'expected_total_cost_per_unit', 'expected_total_margin', 'expected_total_profit', 'invoice_id', 'invoiced', 
    'invoiced_at', 'item_alternate_code_1', 'item_alternate_code_2', 'item_category_name', 'item_class', 'item_code', 'item_customer', 
    'item_description', 'item_family_name', 'item_type_name', 'item_uuid', 'job_comments', 'job_reference', 'job_status', 'job_uuid', 
    'labor_charge', 'labor_charge_per_person_hour', 'labor_charge_per_unit', 'labor_percentage_of_charge', 'line_efficiency', 
    'line_leader_name', 'line_name', 'machine_hours', 'machine_hours_per_unit', 'machine_hours_productive', 'materials_charge', 
    'materials_charge_per_unit', 'nu_fixed_base_i.l._oh_charge', 'nu_fixed_base_i.l._oh_cost', 'nu_g+a_+_corp_oh_charge', 
    'nu_g+a_+_corp_oh_cost', 'nu_var_supp_i.l._oh_charge', 'nu_var_supp_i.l._oh_cost', 'number_of_personnel', 'number_of_time_reports', 
    'overhead_charge', 'overhead_charge_per_unit', 'overhead_cost', 'overhead_cost_per_unit', 'performance', 'po_line_item_number', 
    'project_code', 'project_description', 'project_id', 'project_reference_1', 'project_reference_2', 'project_reference_3', 
    'purchase_order_number', 'reconciled_at', 'reconciliation_status', 'scenario_name', 'service_category_name', 'signed_off', 
    'site_name', 'standard_people', 'standard_person_hours', 'standard_person_hours_per_unit', 'standard_units_per_hour', 'total_charge', 
    'total_charge_per_unit', 'unit_of_measure', 'units_expected', 'units_produced', 'units_remaining']
    filters = [{'column': 'actual_job_start_at', 'operator': 'between', 'from_threshold': from_threshold,
                                                                        'to_threshold': to_threshold}]
    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    # Item Master lookup table
    report_code = 'item_master'
    columns = ['code', 'base_unit_of_measure']
    item_base_unit_lookup = nulogy.get_report(report_code=report_code, columns=columns, headers=False)
    item_base_unit_lookup = {item_code: base_unit_of_measure for item_code, base_unit_of_measure in item_base_unit_lookup}


    headers = next(report)
    headers.append('Base unit of measure')
    headers.append('Base units produced')
    report = [row for row in report]

    # remove old records
    sql.execute(f"DELETE FROM factJobProfitability WHERE [Actual Job start date] BETWEEN '{from_threshold}' AND '{to_threshold}'")

    for row in report:
        row.append('')
        row.append('')
        row = {k: v for k, v in zip(headers, row)}

        row['Base unit of measure'] = item_base_unit_lookup[row['Item code']]
        row['Base units produced'] = nulogy.convertToBaseUnits(row['Item code'], row['Unit of measure'], row['Units produced'])

        sql.insert(table='factJobProfitability', record=row)
    
async def process_labor_report (days: int=28) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d 00:00")
    to_threshold = timestamp.strftime("%Y-%m-%d 23:59")
    
    report_code = 'labor'
    columns = ['availability', 'badge_code', 'badge_type_name', 'badge_type_prefix', 'badge_type_rate', 
               'clock_in_at', 'clock_out_at', 'customer_name', 'duration', 'item_alternate_code_1', 'item_alternate_code_2', 
               'item_category_name', 'item_code', 'item_description', 'item_family_name', 'item_type_name', 'job_id', 
               'job_reference', 'line_efficiency', 'line_leader_name', 'line_name', 'payable_hours', 'performance', 
               'productive_hours', 'project_code', 'project_id', 'reference_1', 'reference_2', 'reference_3', 'site_name']
    filters = [{'column': 'clock_in_at', 'operator': 'between', 'from_threshold': from_threshold,
                                                                'to_threshold': to_threshold}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    report = [row for row in report]

    # remove old records
    sql.execute(f"DELETE FROM factLabor WHERE [Clock in time] BETWEEN '{from_threshold}' AND '{to_threshold}'")

    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert(table='factLabor', record=row)

async def process_weekly_comsumption() -> None:
    #TODO Implement me    
    pass

async def process_inventory_snapshot () -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))

    report_code = "inventory_snapshot"
    columns = ["pallet_number", "item_type", "customer_name"]
    report = nulogy.get_report(report_code=report_code, columns=columns)
    field_names = next(report)
    df = pd.DataFrame.from_records(report, columns=field_names)


    report_code = "pallet_aging"
    columns = ["location", "pallet_number"]
    report = nulogy.get_report(report_code=report_code, columns=columns)
    field_names = next(report)
    pallet_aging_df = pd.DataFrame.from_records(report, columns=field_names)
    pallet_aging_df.rename(columns={"Pallet number": "Pallet Number"}, inplace=True)  # Rename column to match inventory_snapshop_df


    df = df.merge(pallet_aging_df, on='Pallet Number', how='left')
    df['timestamp'] = timestamp.strftime("%Y-%m-%d %H:%M")

    df.drop_duplicates(inplace=True)
    logging.info(f"Inserting {len(df)} rows into factInventory")
    df.to_sql('factInventory', sql.engine, if_exists='append', index=False, chunksize=1000)

    
    logging.info('Processing inventory summary')

    # Add Warehouse column
    df['Warehouse'] = 'Other'
    df.fillna(value={'Location': ''}, inplace=True)
    df.loc[map(lambda l: l.startswith('B')          , df['Location']), 'Warehouse'] = 'Burnett'
    df.loc[map(lambda l: l.startswith('L')          , df['Location']), 'Warehouse'] = 'Locust'
    df.loc[map(lambda l: l.startswith('D')          , df['Location']), 'Warehouse'] = 'Dixie'
    df.loc[map(lambda l: l.startswith('Line')       , df['Location']), 'Warehouse'] = 'Dixie'
    df.loc[map(lambda l: l.startswith('LW')         , df['Location']), 'Warehouse'] = 'Locust West'
    df.loc[map(lambda l: l.startswith('LOCUST WEST'), df['Location']), 'Warehouse'] = 'Locust West'


    summary_df = df[['timestamp', 'Warehouse', 'Pallet Number']].drop_duplicates()
    summary_df = summary_df.groupby(['timestamp', 'Warehouse']).count()
    summary_df.reset_index(inplace=True)
    summary_df.rename(columns={'timestamp': 'Date','Pallet Number': 'Pallet Count'}, inplace=True)

    logging.info(f"Inserting {len(summary_df)} rows into factInventorySummary")
    summary_df.to_sql('factInventorySummary', sql.engine, if_exists='append', index=False)



async def main(mytimer: func.TimerRequest) -> None:
    est_timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))

    if mytimer.past_due:
        logging.info('The timer is past due!')

    await asyncio.gather(
        process_ship_orders(),
        process_shipments(),
        process_receipts(),
        process_moves(),
        process_picks(),
        process_labor_report(),
        process_invoice_report(),
        process_job_profitability_report(),
        process_inventory_snapshot()
    )

    logging.info('Python timer trigger function ran at %s', est_timestamp)
