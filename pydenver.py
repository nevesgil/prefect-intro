from prefect import task, Flow


@task
def hello_world():
    print("Hello world!")
    return "Hello Prefect!"


@task
def prefect_say(s: str):
    print(s)


def build_flow():
    with Flow("my first flow") as flow:
        r = hello_world()
        s2 = prefect_say(r)
    return flow


"""
I'm gonna use the build_flow function instead of calling the flow.run straight form the code.
"""
flow = build_flow()

flow.run()
