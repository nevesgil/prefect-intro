from prefect import task, Flow


@task
def hello_world():
    print("Hello world!")


def build_flow():
    with Flow("my first flow") as flow:
        r = hello_world()
    return flow


flow = build_flow()

flow.run()
