import pandas as pd

from prefect import task, flow
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)
def extract():
    return pd.DataFrame([[1,2,3], [4,5,6]], columns=list('abc'))


@task(cache_key_fn=task_input_hash)
def transform(df):
    return df['a'] + df['b']


@task(cache_key_fn=task_input_hash)
def load(df):
    print(df)


@flow
def my_flow():
    raw = extract()
    processed = transform(raw)
    load(processed)


if __name__ == '__main__':
    my_flow()
