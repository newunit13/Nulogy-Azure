import azure.functions as func
import utils.nulogy as nu
import datetime
import pytz
import logging
import requests
import json
from typing import List

# Const to set the maximum age of a pallet in minutes before notification is sent.
MAX_PALLET_AGE_MINUTES = 15

class EST(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours = -5)
    
    def tzname(self, dt):
        return "EST"
    
    def dst(self, dt):
        return datetime.timedelta(0)

class EDT(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours = -4)
    
    def tzname(self, dt):
        return "EDT"
    
    def dst(self, dt):
        return datetime.timedelta(0)

def format_message(data: List[list]) -> str:
    """
    Format the data into an HTML table for the email message.
    """
    msg = """
    <html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=us-ascii">
        <style>
            table, th, td {
                border: 1px solid black;
                border-collapse: collapse;
                padding-left: 15px;
                padding-right: 15px;
            }
        </style>
    </head>
    <body>
        <table style="75%">
        <tr>
            <th>Location</th>
            <th>Pallet</th>
            <th>Item #</th>
            <th>Time in storage (in minutes)</th>
            <th>Full Pallet %</th>
        </tr>"""
    for line in data:            
        location   = line[0]
        pallet     = line[1]
        item       = line[2]
        age        = int(line[3])
        pallet_pct = float(line[4])

        msg += f"""
        <tr>
          <td>{location}</td>
          <td>{pallet}</td>
          <td>{item}</td>
          <td style="text-align: center">{age:,}</td>
          <td style="text-align: center">{pallet_pct:.0%}</td>
        </tr>"""

    msg += """
        </table>
    </body>
    </html>"""
    return msg

def get_aged_pallets(report: List[list]) -> int:
    """
    Get the number of pallets that are older than MAX_PALLET_AGE_MINUTES.
    """
    aged_pallet_count = 0
    for line in report:
        if int(line[3]) > MAX_PALLET_AGE_MINUTES:
            aged_pallet_count += 1
    return aged_pallet_count

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')

    report_code = "pallet_aging"
    columns = ["location", "pallet_number", "item_code", "time_in_storage_minutes", "full_pallet_quantity"]
    filters = [{"column": "location", "operator": "starts with", "threshold": "Line"}, 
                {"column": "item_type_name", "operator": "starts with", "threshold": "F"}]

    report = nu.get_report(report_code=report_code, columns=columns, filters=filters, headers=False)

    dashboard_url = 'https://api.powerbi.com/beta/49705843-c33c-42c0-aced-f21acaabd4fc/datasets/e69d4235-d54d-4cb1-a026-7bbd9215c27a/rows?key=iDdR5%2FDOVOF4chFYQ5A1NZcDDNe5Jde8S7aBMzDsoVyEL%2B%2By8SM4H%2FJior5ZyRgNrIrQfFL5b2xxNetGuVU7aA%3D%3D'
    dashboard_headers = {
        "Content-Type": "application/json"
    }

    data = []
    for line in report:
        data.append({
            "location"                  : line[0],
            "pallet_number"             : line[1],
            "item_code"                 : line[2],
            "time_in_storage_minutes"   : int(line[3]),
            "full_pallet_quantity"      : f"{float(line[4]):0.0%}",
            "timestamp"                 : datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%m/%d/%Y %H:%M:%S"),
        })

    if not data:
        data.append({
            "location"                  : '',
            "pallet_number"             : '',
            "item_code"                 : '',
            "time_in_storage_minutes"   : 0,
            "full_pallet_quantity"      : ''
        })
    data = json.dumps(data)
    r = requests.post(url=dashboard_url, headers=dashboard_headers, data=data)

    logging.info(f'Push to PowerBI Status Code: {r.status_code}')
