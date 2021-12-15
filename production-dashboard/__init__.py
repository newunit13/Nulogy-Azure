import azure.functions as func
import utils.nulogy as nu
import datetime
import pytz
import logging
import requests
import json

class EST(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours = -5)
    
    def tzname(self, dt):
        return "EDT"
    
    def dst(self, dt):
        return datetime.timedelta(0)

class EDT(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours = -4)
    
    def tzname(self, dt):
        return "EDT"
    
    def dst(self, dt):
        return datetime.timedelta(0)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    report_code = "job_productivity"
    columns = ["line_name", "line_leader_name", "units_produced", "units_remaining", "unit_of_measure", "performance", "availability", "line_efficiency", "percent_complete"]
    filters = [{"column": "actual_job_start_at", "operator": "today"}, {"column": "job_status", "operator": "=", "threshold": "started"}]
    sort_by = [{"column": "line_name", "direction": 'asc'}]

    report = nu.get_report(report_code=report_code, columns=columns, filters=filters, sort_by=sort_by, headers=False)

    dashboard_url = 'https://api.powerbi.com/beta/49705843-c33c-42c0-aced-f21acaabd4fc/datasets/ff1b61c5-421a-4424-a596-35d68348df75/rows?key=G6DhhScPHvGSTC7WqUVA1KV2IYzDUk21toQcGmYo9jzvTdSAwV7tqhICbF9f4%2FwQ2PIMdx5zWpfh9Z%2Fbsl95fg%3D%3D'
    dashboard_headers = {
        "Content-Type": "application/json"
    }

    data = []
    for line in report:
        data.append({
            "line_name"         : line[1],
            "line_leader"       : line[2],
            "units_produced"    : float(line[3]),
            "units_remaining"   : float(line[4]) if float(line[4]) >= 0 else 0,
            "unit_of_measure"   : line[5],
            "line_performance"  : float(line[6].replace('%','')),
            "line_availability" : float(line[7].replace('%','')),
            "line_effeciency"   : float(line[8].replace('%','')),
            "timestamp"         : datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%m/%d/%Y %H:%M:%S"),
            "percent_complete"  : f"{float(line[9].replace('%','')):0.0f}%",
            "job_id"            : line[0]
        })

    if not data:
        data.append({
            "line_name" :"",
            "line_leader" :"",
            "units_produced" :0,
            "units_remaining" :0,
            "unit_of_measure" :"",
            "line_performance" :0,
            "line_availability" :0,
            "line_effeciency" :0,
            "timestamp" :datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%m/%d/%Y %H:%M:%S"),
            "percent_complete" :"",
            "job_id" :""
        })
    data = json.dumps(data)
    r = requests.post(url=dashboard_url, headers=dashboard_headers, data=data)

    logging.info(f'Push to PowerBI Status Code: {r.status_code}')
