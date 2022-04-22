import csv
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
import datetime


@task(max_retries=2, retry_delay=datetime.timedelta(seconds=2))
def extract(path):
    with open(path, "r") as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    print(data)
    return data


@task
def transform(data):
    tdata = [i + 1 for i in data]
    return tdata


@task
def load(data, path):
    with open(path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)


def build_flow(schedule):
    with Flow("my_etl", schedule=schedule) as flow:
        path = Parameter(name="path", required=True)
        data = extract(path)
        tdata = transform(data)
        result = load(tdata, path)
    return flow


schedule = IntervalSchedule(
    start_date=datetime.datetime.now() + datetime.timedelta(seconds=1),
    interval=datetime.timedelta(seconds=1),
    end_date=datetime.datetime.now() + datetime.timedelta(seconds=11),
)


flow = build_flow(schedule)

flow.run(parameters={"path": "values.csv"})
