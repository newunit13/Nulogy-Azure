from utils.config import NULOGY_SECRET_KEY
import utils.sql as sql
from typing import List, Dict, Tuple
from time import sleep
from random import randint
import datetime
import requests
import json
import csv
import logging

uoms = None
utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

def process_report(report_code: str, filter_column: Tuple[str, str], database_table: str, days: int=28) -> None:

    timestamp = datetime.datetime.now()
    from_threshold = (timestamp - datetime.timedelta(days=days)).strftime("%Y-%m-%d 00:00")
    to_threshold = timestamp.strftime("%Y-%m-%d 23:59")


    nulogy_reports = json.loads(open('nulogy_reports.json').read())
    columns = [column for column in nulogy_reports[report_code].keys()]
    filters = [{'column': filter_column, 'operator': 'between', 'from_threshold': from_threshold, 
                                                                'to_threshold': to_threshold}]

    report = get_report(report_code=report_code, columns=columns, filters=filters)

    # remove old records
    sql.execute(f"""DELETE FROM {database_table} 
                    WHERE [{nulogy_reports[report_code][filter_column]}] 
                    BETWEEN '{from_threshold}' AND '{to_threshold}'""")

    headers = next(report)
    for row in report:
        row = {k: v for k, v in zip(headers, row)}
        sql.insert(table='factShipOrder', record=row)


def downlad_report(download_url: str) -> str:

    response = requests.get(download_url)

    if response.status_code != 200:
        logging.error(f'Error downloading report code {response.status_code}: {response.text}')
        raise Exception(f"Invalid Status code downloading report: {response.status_code}")
    
    return response.content.decode('utf-8')


def poll_report_url(url: str) -> str:

    headers = {
        "Authorization": f"Basic {NULOGY_SECRET_KEY}",
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json"
    }

    response = requests.get(url=url, headers=headers)

    if response.status_code != 200:
        logging.error(f'Polling error code {response.status_code}: {response.text}')
        raise Exception(f"Error in polling: {response.status_code}")

    while response.json()['status'] != 'COMPLETED':
        logging.info(f"Report status {response.json()['status']}, sleeping 10 seconds before next poll")
        sleep(10)
        response = requests.get(url=url, headers=headers)
        if response.json()['status'] == 'FAILED':
            logging.error(f"Report failed: {response.json()['status']}")
            raise Exception(f"Report failed: {response.json()['status']}")

    return response.json()['url']

def get_report(report_code: str, columns: List[str], filters: List[dict]=[], sort_by: List[dict]=[], headers: bool=True):
    url = "https://app.nulogy.net/api/reports/report_runs"

    _headers = {
        "Authorization": f"Basic {NULOGY_SECRET_KEY}",
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json"
    }

    _data = json.dumps({
        "report" : report_code,
        "columns" : columns,
        "filters": filters,
        "sort_by": sort_by
    })

    error_count = 0
    while True:
        logging.info(f"Submitting request for {report_code} report")
        response = requests.post(url=url, headers=_headers, data=_data)

        if response.status_code == 201:
            break
        
        if response.status_code != 201:
            logging.error(f'Get report error {response.status_code}: {response.text}')
            if error_count > 3:
                logging.error(f'Too many failed attempts. Exiting')
                raise Exception
            error_count += 1
        sleep(randint(60,120))

    # small sleep to give the report a chance to generate before polling for a download link
    sleep(15)

    try:
        status_url = response.json()['status_url']
        result_url = poll_report_url(status_url)
        report = downlad_report(result_url)
        report = [line for line in report.split('\n') if line]      # remove blank lines

        report = csv.reader(report, delimiter=',', quotechar='"')

    except Exception as e:
        logging.error(f'EXCEPTION-{report_code}-{utc_timestamp}: {e}')
    
    if not headers:
        next(report)

    return report

def get_uom_list() -> Dict:
    report_code = "uom_ratios"
    columns = ["code", "unit_of_measure", "ratio", "conversion_unit_of_measure"]

    report = get_report(report_code=report_code, columns=columns, headers=False)

    uoms = dict()
    for line in report:
        item_code   = line[0]
        from_unit   = line[1]
        factor      = float(line[2])
        to_unit     = line[3]

        if item_code not in uoms:
            uoms[item_code] = {
                "conversions" : [{"from_unit": from_unit, "factor": factor, "to_unit": to_unit}]
            }
        else:
            uoms[item_code]["conversions"].append({"from_unit": from_unit, "factor": factor, "to_unit": to_unit})

    report_code = "item_master"
    columns = ["code", "base_unit_of_measure"]
    filters = [{"columns": "inactive", "operator": "=", "threshold": "False"}]

    report = get_report(report_code=report_code, columns=columns, headers=False)

    for line in report:
        item_code = line[0]
        base_unit = line[1]

        if item_code in uoms:
            uoms[item_code].update({"base_unit": base_unit})
        else:
            uoms[item_code] = {
                "base_unit"     : base_unit,
                "conversions"   : [{"from_unit": base_unit, "factor": 1, "to_unit": base_unit}]
            }
    return uoms

def convertToBaseUnits(item_code: str, unit_of_measure: str, number_of_units: float) -> float:
    """
        Returns the number of base units for a specified item.

            Parameter:
                item_code (str)         : The Nulogy Item Code
                unit_of_measure (str)   : The unit of measure to be converted from
                number_of_units (float) : The number of units to be converted into base units

            Returns:
                number_of_units (float) : The number of base units 
    """

    short_to_long_uom = {
        'ea'    : 'eaches',
        'cs'    : 'cases',
        'pl'    : 'pallets',
        'rl'    : 'rolls',
        'lbs'   : 'pounds',
        'bdl'   : 'bundles',
        'pk'    : 'packs',
        'kg'    : 'kilograms',
        'ltr'   : 'liters',
        'box'   : 'boxes',
        'gal'   : 'gallons',
        'ft'    : 'feet'
    }
    
    global uoms
    if uoms == None:
        uoms = get_uom_list()

    # if a short code UOM is supplied convert it over to the long form
    if unit_of_measure in short_to_long_uom:
        unit_of_measure = short_to_long_uom[unit_of_measure]

    # if the number of units wasn't in a numeric type convert it
    if not isinstance(number_of_units, float):
        number_of_units = float(number_of_units)

    item_base_unit = uoms[item_code]["base_unit"]
    if unit_of_measure == item_base_unit:
        return number_of_units

    for conversion in uoms[item_code]["conversions"]:
        if conversion["from_unit"] == unit_of_measure:
            number_of_units = number_of_units * conversion["factor"]
            unit_of_measure = conversion["to_unit"]
            break
    else:
        raise Exception(f"Unable to find {unit_of_measure} in {item_code} conversions.")

    return convertToBaseUnits(item_code, unit_of_measure, number_of_units)


if __name__ == '__main__':

    report_code = "pallet_aging"
    columns = ["location", "pallet_number", "item_code", "time_in_storage_minutes"]
    filters = [{"column": "location", "operator": "starts with", "threshold": "Line"}, 
               {"column": "item_type_name", "operator": "starts with", "threshold": "F"}]

    report = get_report(report_code=report_code, columns=columns, filters=filters)

