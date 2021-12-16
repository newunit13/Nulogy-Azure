import datetime
import logging

import azure.functions as func
from utils import sql, nulogy as nu


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')


    # Run job profitability report to get all jobs that have no been invoiced. Include [Actual Start Date]
    report_code = "job_profitability"
    columns = ["actual_job_start_at"]
    filters = [{"column": "invoiced", "operator": "=", "threshold": "false"}]

    report = nu.get_report(report_code, columns, filters, headers=False)

    jobs_not_invoiced = {}
    for line in report:
        if not line[1]:
            continue

        jobs_not_invoiced[line[0]] = datetime.datetime.strptime(line[1], "%Y-%b-%d %I:%M %p").date()

    # Run job productivity report filtered to min/max start date from job profitability report.

    earliest_job = min(jobs_not_invoiced.values()).strftime('%Y-%b-%d 00:00 AM')
    latest_job = max(jobs_not_invoiced.values()).strftime('%Y-%b-%d 11:59 PM')

    report_code = "job_productivity"
    columns = ["line_name", "line_leader_name", "actual_job_end_at", "project_customer", "item_code", "units_expected", "units_produced", "pallets_produced", "standard_people", "number_of_personnel", "actual_person_hours", "line_efficiency"]
    filters = [{"column": "actual_job_start_at", "operator": "between", "from_threshold": earliest_job, "to_threshold": latest_job }]
    sort_by = [{"column": "line_name", "direction": 'asc'}]


    report = nu.get_report(report_code=report_code, columns=columns, filters=filters, sort_by=sort_by, headers=False)

    production_records = {}
    for line in report:
        production_records[line[0]] = {
            "Job ID"                : line[0],
            "Line Name"             : line[1],
            "Line Leader"           : line[2],
            "Actual Job End Date"   : line[3],
            "Work Order Customer"   : line[4],
            "Item Code"             : line[5],
            "Units Expected"        : float(line[6]),
            "Units Produced"        : float(line[7]),
            "Pallets Produced"      : float(line[8]),
            "Standard People"       : float(line[9]),
            "Actual People"         : float(line[10]),
            "Actual Hours"          : float(line[11]),
            "Line Efficiency"       : line[12]
        }

    # Match jobs from job productivity report to jobs from job profitability report.
    production_records = {k:v for k,v in production_records.items() if k in jobs_not_invoiced}

    # Check database for job ids that are in dataset.
    for job_id, details in production_records.items():

        sql.insert_or_update('PRODUCTION_RECORDS', 'Job ID', details)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
