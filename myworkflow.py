import csv
from email.policy import default
from prefect import task, Flow, Parameter


@task
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


def build_flow():
    with Flow("my_etl") as flow:
        path = Parameter(name="path", required=True)
        data = extract(path)
        tdata = transform(data)
        result = load(tdata, path)

    return flow


flow = build_flow()

flow.run(parameters={"path": "values.csv"})
