import csv
import string
import random


def load_job_execution_details(job_start_timestamp, job_end_timestamp, job_status):
    execution_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    fields = ['job_start_timestamp', 'job_end_timestamp', 'execution_id', 'job_status']
    rows = [[job_start_timestamp, job_end_timestamp, execution_id, job_status]]
    filename = "execution_log.csv"
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows)


if __name__ == '__main__':
    load_job_execution_details('2022-11-17 15:05:30.859070', '2022-11-17 15:05:30.859070', 'SUCCESS')
