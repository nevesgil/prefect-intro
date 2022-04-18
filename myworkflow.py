import csv
from prefect import task, Flow


@task
def extract(path):
    with open(path, "r") as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    print(data)
    return data
%

@task
def transform(data):
    tdata = [i + 1 for i in data]
    return tdata


@task
def load(data, path):
    with open(path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)


with Flow("my_etl") as flow:
    data = extract("values.csv")
    tdata = transform(data)
    result = load(tdata, "tvalues.csv")
    data2 = extract("values.csv", upstream_tasks=[result])

flow.run()
