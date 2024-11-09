import os
import re

import pandas as pd

years = [2015, 2016, 2017, 2018, 2019, 2020]
INPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "input"
)
map_years = [2021, 2022, 2023, 2024]
COL_MAP = {
    "taxpayer_1": "taxpayer1",
    "taxpayer_2": "taxpayer2",
    "taxpayer 2": "taxpayer2",
}

SPACE_RE = r"\s+"
CLEAN_RE = r"(\.|,)"
EXCLUDE_TAXPAYER_LIST = ["HUD"]
EXCLUDE_RE = r"LAND BANK|CITY OF DETROIT|DETROIT PARKS|BRIDGE AUTHORITY|MDOT|DEPARTMENT OF|DEPT OF|UNK_|UNIDENTIFIED|UNKNOWN|TRUST|HENRY FORD|UAT|UAW|DTE|FCA|WAYNE COUNTY|NON\-PROFIT|TAXPAYER|RECOVERYPARK|RECOVERY PARK|VHS|HARPER\-HUTZEL|POPE FRANCIS|DETROIT MERCY|CATHEDRAL|PARISH|PERFECTING CHURCH"  # noqa


def clean_own_id(own_id):
    return re.sub(r"\s+", " ", own_id.upper()).strip()


if __name__ == "__main__":
    df_list = []
    for year in years:
        print(year)
        df = pd.read_csv(
            os.path.join(INPUT_DIR, "praxis_csvs", f"PPlusFinal_{year}_edit.csv"),
        ).rename(columns=COL_MAP)[["taxpayer1", "taxpayer2", "own_id"]]
        df = df[~pd.isnull(df["own_id"])].drop_duplicates()
        df_list.append(df)
    for year in map_years:
        print(year)
        df = pd.read_csv(os.path.join(INPUT_DIR, f"own-id-{year}.csv")).rename(
            columns={"taxpayer": "taxpayer1", "owner": "own_id"}
        )
        df = df[~pd.isnull(df["own_id"])].drop_duplicates()
        df_list.append(df)
    df = pd.concat(df_list).drop_duplicates()
    df["own_id"] = df["own_id"].apply(clean_own_id)
    df["own_id"] = df["own_id"].apply(
        lambda x: 'MANUEL "MATTY" MOROUN'
        if ((("MANUEL" in x) or ("MATTHEW" in x)) and ("MOROUN" in x))
        else x
    )
    df["own_id"] = df["own_id"].apply(
        lambda x: "SALAMEH JASER" if (("SALAMEH" in x) and ("JASER" in x)) else x
    )
    df["own_id"] = df.apply(
        lambda x: "MATTHEW TATARIAN"
        if (
            (x["taxpayer1"] in ["TATARIAN, MATTHEW M", "TATARIAN MATTHEW M"])
            or ("TATARIAN MATTHEW" in x["taxpayer1"])
        )
        else x["own_id"],
        axis=1,
    )
    df["own_id"] = df.apply(
        lambda x: "JAMAL ALKIBBI" if "ALFA KIBBI" in x["taxpayer1"] else x["own_id"],
        axis=1,
    )
    df = df[
        ~(
            df.own_id.str.contains(EXCLUDE_RE, regex=True)
            | df.taxpayer1.str.contains(EXCLUDE_RE, regex=True)
            | df.taxpayer2.str.contains(EXCLUDE_RE, regex=True)
            | df.taxpayer1.isin(EXCLUDE_TAXPAYER_LIST)
        )
    ]
    df.to_csv(os.path.join(INPUT_DIR, "own-id-map.csv"), index=False)
