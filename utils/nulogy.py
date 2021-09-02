from utils.config import NULOGY_SECRET_KEY
from typing import List, Dict
from time import sleep
from datetime import datetime
from random import randint
import requests
import json
import csv

uoms = None

def filename_with_timestamp(report_type: str) -> str:
    """
        Returns a string timestamped string with the report type and .csv extension

            Parameter:
                report_type (str)   : The type of report

            Returns:
                str : '<timestamp>-<report_type>.csv'
    """
    return f"{datetime.now().strftime('%Y%m%d-%H%M')}-{report_type}.csv"


def downlad_report(download_url: str) -> str:

    response = requests.get(download_url)

    if response.status_code != 200:
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
        raise Exception(f"Error in polling: {response.status_code}")

    while response.json()['status'] != 'COMPLETED':
        print('Report not yet ready, sleeping 10 seconds before next poll')
        sleep(10)
        response = requests.get(url=url, headers=headers)

    return response.json()['url']

def get_report(report_code: str, columns: List[str], filters: List[dict]=[], sort_by: List[dict]=[], headers: bool=True) -> bool:
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
        print(f"Submitting request for {report_code} report")
        response = requests.post(url=url, headers=_headers, data=_data)

        if response.status_code == 201:
            break
        
        if response.status_code != 201:
            with open(F'ERROR-{report_code}-{datetime.now().strftime("%Y%m%d-%H%M")}.txt', 'w') as outfile:
                outfile.write(f'{response.status_code}-{response.text}')
                outfile.write(f'Error count: {error_count}')
                if error_count > 3:
                    outfile.write(f'Too many failed attempts. Exiting')
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
        with open(F'EXCEPTION-{report_code}-{datetime.now().strftime("%Y%m%d-%H%M")}.txt', 'w') as outfile:
            outfile.write(f'{e}')
    
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

