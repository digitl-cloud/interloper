import pandas as pd

from interloper_pandas import DataframeNormalizer


def test_x():
    data = [
        {"a": 1, "b": 2, "c": {"d": 3}, "e": {"f": {"g": 4}}},
    ]
    df1 = pd.DataFrame(data)
    df2 = pd.json_normalize(data, sep="_", max_level=0)

    print(df1)
    print(df2)
    # print(pd.json_normalize(df1.to_dict(orient="records"), sep="_"))
    # print(df2)

    assert True
