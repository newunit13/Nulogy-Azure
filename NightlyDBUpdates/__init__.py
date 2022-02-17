import datetime
import logging
import pytz
import asyncio
from utils import nulogy, sql
import azure.functions as func


async def process_shipments (days: int=7) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    report_code = 'shipment_item'
    columns = ['actual_ship_at', 'ship_order_code', 'bill_of_lading_number', 'ship_order_shipped', 
               'ship_order_customer_name', 'carrier_code', 'trailer_number', 'ship_from', 'ship_to', 
               'item_code', 'pallet_number', 'case_quantity', 'shipment_invoiced']
    filters = [{'column': 'actual_ship_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"), 
                                                                   'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")},
               {'column': 'shipment_invoiced', 'operator': '=', 'threshold': 'No'}]
               
    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert_or_update(table='factShipment', key=['Shipment', 'Pallet number', 'Item code'], record=row)

async def process_receipts (days: int=7) -> None:
    
    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    report_code = 'receipt_item'
    columns = ['received_at', 'item_customer_name', 'location_name', 'receipt_reference_1',
               'receipt_carrier_code', 'receipt_status', 'trailer_or_container', 'item_category_name', 'full_pallet_quantity',
               'case_quantity', 'item_code', 'item_description', 'pallet_number']
    filters = [{'column': 'received_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"),
                                                                    'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert_or_update(table='factReceipt', key=['Receipt', 'Pallet number', 'Item code'], record=row)

async def process_moves (days: int=2) -> None:
    
    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    report_code = 'move_transaction'
    columns = ['assigned_to', 'from_location', 'from_pallet_number', 'to_location', 'to_pallet_number', 'time_completed_at', 'item_code', 'base_quantity', 'base_unit_of_measure']
    filters = [{'column': 'time_completed_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"),
                                                                      'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")}]
    
    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert_or_update(table='factMove', key=['Move', 'From pallet number', 'To pallet number', 'Item code'], record=row)

async def process_invoice_report (days: int=7) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    report_code = 'invoice'
    columns = ['alternate_code_1', 'alternate_code_2', 'bill_to', 'charge_per_unit', 'customer_code', 'customer_name',
               'customer_notes', 'internal_notes', 'invoice_status', 'invoiced_at', 'item_category_name', 'item_code', 
               'item_description', 'item_family_name', 'item_type_name', 'job_id', 'notes', 'paid_at', 'payment_due_on', 
               'po_line_item_number', 'project_code', 'project_id', 'project_reference_1', 'project_reference_2', 
               'project_reference_3', 'purchase_order_number', 'reference_1', 'reference_2', 'shipment_id', 'site_name', 
               'terms', 'total_charge', 'unit_of_measure', 'unit_quantity']
    filters = [{'column': 'invoiced_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"),
                                                                      'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    headers.append('Timestamp')
    report = [row for row in report]
    for row in report:
        row.append(timestamp.strftime("%Y-%m-%d %H:%M"))
        row = {k: v for k, v in zip(headers, row)}
        sql.insert_or_update(table='factInvoice', key=headers[:-1], record=row)

async def process_job_profitability_report (days: int=7) -> None:
    
    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
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
    filters = [{'column': 'actual_job_start_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"),
                                                                        'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    headers.append('Timestamp')
    report = [row for row in report]
    report_len = len(report)
    for i, row in enumerate(report):
        row.append(timestamp.strftime("%Y-%m-%d %H:%M"))
        row = {k: v for k, v in zip(headers, row)}
        logging.info(f'Processing row {i+1} of {report_len}')
        sql.insert_or_update(table='factJobProfitability', key=headers[:-1], record=row)
    
async def process_labor_report (days: int=7) -> None:

    timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))
    report_code = 'labor'
    columns = ['availability', 'badge_code', 'badge_type_name', 'badge_type_prefix', 'badge_type_rate', 
               'clock_in_at', 'clock_out_at', 'customer_name', 'duration', 'item_alternate_code_1', 'item_alternate_code_2', 
               'item_category_name', 'item_code', 'item_description', 'item_family_name', 'item_type_name', 'job_id', 
               'job_reference', 'line_efficiency', 'line_leader_name', 'line_name', 'payable_hours', 'performance', 
               'productive_hours', 'project_code', 'project_id', 'reference_1', 'reference_2', 'reference_3', 'site_name']
    filters = [{'column': 'clock_in_at', 'operator': 'between', 'from_threshold': (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M"),
                                                                        'to_threshold': timestamp.strftime("%Y-%m-%d %H:%M")}]

    report = nulogy.get_report(report_code=report_code, columns=columns, filters=filters)

    headers = next(report)
    headers.append('Timestamp')
    report = [row for row in report]
    for row in report:
        row.append(timestamp.strftime("%Y-%m-%d %H:%M"))
        row = {k: v for k, v in zip(headers, row)}
        sql.insert_or_update(table='factLabor', key=headers[:-1], record=row)
    

async def main(mytimer: func.TimerRequest) -> None:
    est_timestamp = datetime.datetime.now(pytz.timezone('US/Eastern'))

    if mytimer.past_due:
        logging.info('The timer is past due!')

    await process_shipments()
    await process_receipts()
    await process_moves()
    await process_labor_report()
    await process_invoice_report()
    await process_job_profitability_report()

    logging.info('Python timer trigger function ran at %s', est_timestamp)
