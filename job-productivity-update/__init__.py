import datetime
import logging

import azure.functions as func
from utils import sql, nulogy as nu


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')


    # TODO: Run job profitability report to get all jobs that have no been invoiced. Include [Actual Start Date]
    report_code = "job_profitability"
    columns = ["actual_job_start_at"]
    filters = [{"column": "invoiced", "operator": "=", "threshold": "false"}]

    report = nu.get_report(report_code, columns, filters, headers=False)

    data = []
    for line in report:
        if line[1]:
            data.append(
                (line[0], datetime.datetime.strptime(line[1], "%Y-%b-%d %I:%M %p").date())
            )

    # TODO: Run job productivity report filtered to min/max start date from job profitability report.

    report_code = "job_productivity"
    columns = ["line_name", "line_leader_name", "units_produced", "units_remaining", "unit_of_measure", "performance", "availability", "line_efficiency", "percent_complete"]
    filters = [{"column": "actual_job_start_at", "operator": "between", "from_threshold": min(), "to_threshold": "" }]
    sort_by = [{"column": "line_name", "direction": 'asc'}]

    
    report = nu.get_report(report_code=report_code, columns=columns, filters=filters, sort_by=sort_by, headers=False)

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
            "timestamp"         : datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
            "percent_complete"  : f"{float(line[9].replace('%','')):0.0f}%"
        })

    sql.insert(table="job_productivity", record=data)

    # TODO: Match jobs from job productivity report to jobs from job profitability report.
    
    # TODO: Check database for job ids that are in dataset.
    # if job_id in dataset:
    #   update recod
    # else:
    #  insert record 


    logging.info('Python timer trigger function ran at %s', utc_timestamp)
