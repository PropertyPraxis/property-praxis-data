import os

import pandas as pd

years = [2015, 2016, 2017, 2018, 2019, 2020, 2021]
INPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "input"
)
COL_MAP = {
    "taxpayer_1": "taxpayer1",
    "taxpayer_2": "taxpayer2",
    "taxpayer 2": "taxpayer2",
}

if __name__ == "__main__":
    df_list = []
    for year in years:
        print(year)
        df = pd.read_csv(
            os.path.join(INPUT_DIR, "praxis_csvs", f"PPlusFinal_{year}_edit.csv"),
        ).rename(columns=COL_MAP)[["taxpayer1", "taxpayer2", "own_id"]]
        df = df[~pd.isnull(df["own_id"])].drop_duplicates()
        df_list.append(df)
    df = pd.concat(df_list).drop_duplicates()
    df.to_csv(os.path.join(INPUT_DIR, "own-id-map.csv"), index=False)
