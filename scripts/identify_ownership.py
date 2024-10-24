import csv
import os
import re

import geopandas as gpd
import numpy as np
import pandas as pd

INPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "input"
)
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
)
COL_MAP = {
    "pnum": "parcel_num",
    "parcelno": "parcel_num",
    "parcel_number": "parcel_num",
    "taxpayer_1": "taxpayer1",
    "taxpayer_2": "taxpayer2",
    "saledate": "sale_date",
    "propaddr": "address",
    "addr": "address",
    "owner1": "taxpayer1",
    "owner2": "taxpayer2",
    "owner_street": "taxpayer_address",
    "owner_city": "taxpayer_city",
    "taxpayer_s": "taxpayer_address",
    "taxpayer_c": "taxpayer_city",
    "tpaddr": "taxpayer_address",
    "taxpaddr": "taxpayer_address",
    "taxpcity": "taxpayer_city",
    "tpcity": "taxpayer_city",
    "taxpayer_street": "taxpayer_address",
}
SPACE_RE = r"\s+"
CLEAN_RE = r"(\.|,)"
ADDR_SUFFIX_RE = r" (DR|DRIVE|AVE|AVENUE|ST|STREET|BLVD|BOULEVARD|FWY|FREEWAY)\.?$"
LLC_RE = r" (LLC|INC|CO)\b"
DETROIT_RE = r"LAND BANK|CITY OF DETROIT|DETROIT HOUSING COMMISSION|DETROIT WATER SEWERAGE|DETROIT FIRE|DETROIT PUBLIC|SCHOOLS|DETROIT PARKS|City of Detroit|BRIDGE AUTHORITY|MDOT|DEPARTMENT OF|DETROIT CITY AIRPORT|FANNIE MAE|BROWNFIELD|DEPT OF HOME|HOMELAND SECURITY|DEPT OF TRANS|RECREATION DEP|WAYNE COUNTY DPS|DOWNTOWN DEVELOPMENT AUTHORITY|WATER AUTHORITY|US POSTAL SERVICE|WAYNE STATE U|STATE OF MICHIGAN"  # noqa


def clean_owner(owner):
    if not isinstance(owner, str):
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9 ]+", "", owner)).strip()


def clean_dates(date_str):
    if not isinstance(date_str, str):
        return date_str
    if "1900" in date_str:
        return np.nan
    try:
        return pd.to_datetime(
            date_str.split()[0],
            format=("%m/%d/%Y" if ("/" in date_str) else "%Y-%m-%d"),
        )
    except Exception:
        return np.nan


def fix_parcelno(parcelno):
    if not ("." in parcelno or "-" in parcelno):
        parcelno = parcelno + "."
    if len(parcelno.split(".")[0]) == 7 and "." in parcelno:
        parcelno = "0" + parcelno
    return parcelno


def compare_21_20():
    gdf_20 = gpd.read_file(
        f"zip://{os.path.join(INPUT_DIR, 'praxis_shapefiles', 'praxis2020.shp.zip')}"
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "taxpayer1",
            "taxpayer2",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]

    own_df_20 = pd.read_csv(
        os.path.join(INPUT_DIR, "praxis_csvs", "PPlusFinal_2020_edit.csv"),
        dtype={"parcel_num": "str", "parcelno": "str"},
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "own_id",
        ]
    ]
    gdf_20["parcel_num"] = gdf_20["parcel_num"].apply(fix_parcelno)
    own_df_20["parcel_num"] = own_df_20["parcel_num"].apply(fix_parcelno)
    own_df_20 = own_df_20[~pd.isnull(own_df_20["own_id"])].drop_duplicates(
        subset=["parcel_num"]
    )

    gdf_20 = gdf_20.drop_duplicates(subset=["parcel_num"])
    df_20 = gdf_20.merge(own_df_20, on="parcel_num", how="left")

    df_20["taxpayer_address"] = (
        df_20["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_20["taxpayer1"] = df_20["taxpayer1"].apply(clean_owner)
    df_20["taxpayer2"] = df_20["taxpayer2"].apply(clean_owner)

    own_df = pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv"))
    own_df["taxpayer1"] = own_df["taxpayer1"].apply(clean_owner)
    own_df["taxpayer2"] = own_df["taxpayer2"].apply(clean_owner)
    own_records = own_df.to_dict(orient="records")

    own_id_map = {}
    for r in own_records:
        if not r["own_id"] or "UNID" in r["own_id"] or "UNK_" in r["own_id"]:
            continue
        if r["taxpayer1"] not in own_id_map:
            own_id_map[r["taxpayer1"]] = r["own_id"]
        if r["taxpayer2"] and r["taxpayer2"] not in own_id_map:
            own_id_map[r["taxpayer2"]] = r["own_id"]

    df_21 = pd.read_csv(
        os.path.join(INPUT_DIR, "praxis_csvs", "PPlusFinal_2021_edit.csv"),
        dtype={"parcel_num": "str"},
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "address",
            "taxpayer1",
            "taxpayer2",
            "own_id",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    df_21["taxpayer1"] = df_21["taxpayer1"].apply(clean_owner)
    df_21["taxpayer2"] = df_21["taxpayer2"].apply(clean_owner)
    df_21["parcel_num"] = df_21["parcel_num"].apply(fix_parcelno)
    df_21.drop_duplicates(subset=["parcel_num"], inplace=True)
    df_21["address"] = (
        df_21["address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_21["taxpayer_address"] = (
        df_21["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )

    to_merge_21_df = (
        df_21[
            [
                "parcel_num",
                "address",
                "taxpayer1",
                "taxpayer2",
                "own_id",
                "taxpayer_address",
                "taxpayer_city",
            ]
        ]
        .rename(
            columns={
                "taxpayer1": "taxpayer1_21",
                "taxpayer2": "taxpayer2_21",
                "own_id": "own_id_21",
                "taxpayer_address": "taxpayer_address_21",
                "taxpayer_city": "taxpayer_city_21",
            }
        )
        .sort_values(by=["parcel_num", "own_id_21"], ascending=True)
        .drop_duplicates(subset=["parcel_num"])
    )
    merge_df = to_merge_21_df.merge(df_20, on="parcel_num", how="left")

    # Don't override own_id if exists already
    def pull_own_id(d):
        if (
            d["own_id_21"]
            and isinstance(d["own_id_21"], str)
            and "UNK_" not in d["own_id_21"]
        ):
            return d["own_id_21"]
        if d["taxpayer1_21"] in own_id_map:
            return own_id_map[d["taxpayer1_21"]]
        if d["taxpayer2_21"]:
            # Try 2 if 1 not a match
            return own_id_map.get(d["taxpayer2_21"])
        return ""

    merge_df["own_id_21"] = merge_df.apply(
        pull_own_id,
        axis=1,
    )
    merge_df["taxpayer_address_21"] = (
        merge_df["taxpayer_address_21"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )

    changed_merge_df = merge_df[
        (merge_df["taxpayer1"] != merge_df["taxpayer1_21"])
        & (~merge_df["taxpayer1_21"].str.contains(DETROIT_RE))
        & (merge_df["taxpayer1_21"] != "HUD")
        & (merge_df["address"] != merge_df["taxpayer_address_21"])
    ]
    changed_merge_df["taxpayer1_21_count"] = changed_merge_df.groupby("taxpayer1_21")[
        "taxpayer1_21"
    ].transform("count")
    changed_merge_df.sort_values(
        by=["taxpayer1_21_count", "taxpayer1_21"], ascending=False, inplace=True
    )
    changed_merge_df.to_csv(
        "owners-2021.csv", quoting=csv.QUOTE_NONNUMERIC, index=False
    )


def compare_22_21():
    df_21 = pd.read_csv(
        os.path.join(INPUT_DIR, "city", "parcels_2021.csv"),
        dtype={"parcel_num": "str", "parcelno": "str"},
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "taxpayer1",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    df_21["parcel_num"] = df_21["parcel_num"].apply(fix_parcelno)
    df_21.drop_duplicates(subset=["parcel_num"], inplace=True)

    own_df_21 = pd.read_csv(
        os.path.join(INPUT_DIR, "praxis_csvs", "PPlusFinal_2021_edit.csv"),
        dtype={"parcel_num": "str"},
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "own_id",
        ]
    ]
    own_df_21["parcel_num"] = own_df_21["parcel_num"].apply(fix_parcelno)
    own_df_21.drop_duplicates(subset=["parcel_num"], inplace=True)

    df_21 = df_21.merge(own_df_21, on=["parcel_num"], how="left")

    df_21["taxpayer_address"] = (
        df_21["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_21["taxpayer1"] = df_21["taxpayer1"].apply(clean_owner)

    own_df = pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv"))
    own_df["taxpayer1"] = own_df["taxpayer1"].apply(clean_owner)
    own_df["taxpayer2"] = own_df["taxpayer2"].apply(clean_owner)
    own_records = own_df.to_dict(orient="records")

    own_id_map = {}
    for r in own_records:
        if not r["own_id"] or "UNID" in r["own_id"] or "UNK_" in r["own_id"]:
            continue
        if r["taxpayer1"] not in own_id_map:
            own_id_map[r["taxpayer1"]] = r["own_id"]

    gdf_22 = gpd.read_file(
        os.path.join(INPUT_DIR, "city", "IPDS 2022", "det_20220000.shp")
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "address",
            "taxpayer1",
            "taxpayer2",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    gdf_22["own_id"] = ""
    gdf_22["taxpayer1"] = gdf_22["taxpayer1"].apply(clean_owner)
    gdf_22["taxpayer2"] = gdf_22["taxpayer2"].apply(clean_owner)
    gdf_22["parcel_num"] = gdf_22["parcel_num"].apply(fix_parcelno)
    gdf_22.drop_duplicates(subset=["parcel_num"], inplace=True)
    gdf_22["address"] = (
        gdf_22["address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    gdf_22["taxpayer_address"] = (
        gdf_22["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )

    to_merge_22_df = (
        gdf_22[
            [
                "parcel_num",
                "address",
                "taxpayer1",
                "taxpayer2",
                "own_id",
                "taxpayer_address",
                "taxpayer_city",
            ]
        ]
        .rename(
            columns={
                "taxpayer1": "taxpayer1_22",
                "taxpayer2": "taxpayer2_22",
                "own_id": "own_id_22",
                "taxpayer_address": "taxpayer_address_22",
                "taxpayer_city": "taxpayer_city_22",
            }
        )
        .sort_values(by=["parcel_num", "own_id_22"], ascending=True)
        .drop_duplicates(subset=["parcel_num"])
    )
    merge_df = to_merge_22_df.merge(df_21, on="parcel_num", how="left")

    # Don't override own_id if exists already
    def pull_own_id(d):
        if (
            d["own_id_22"]
            and isinstance(d["own_id_22"], str)
            and "UNK_" not in d["own_id_22"]
        ):
            return d["own_id_22"]
        if d["taxpayer1_22"] in own_id_map:
            return own_id_map[d["taxpayer1_22"]]
        if d["taxpayer2_22"]:
            # Try 2 if 1 not a match
            return own_id_map.get(d["taxpayer2_22"])
        return ""

    merge_df["own_id_22"] = merge_df.apply(
        pull_own_id,
        axis=1,
    )
    merge_df["taxpayer1_22"] = merge_df["taxpayer1_22"].fillna("")
    merge_df["taxpayer_address_22"] = merge_df["taxpayer_address_22"].fillna("")

    changed_merge_df = merge_df[
        (merge_df["taxpayer1"] != merge_df["taxpayer1_22"])
        & (~merge_df["taxpayer1_22"].str.contains(DETROIT_RE))
        & (merge_df["taxpayer1_22"] != "HUD")
        & (merge_df["address"] != merge_df["taxpayer_address_22"])
    ]
    changed_merge_df["taxpayer1_22_count"] = changed_merge_df.groupby("taxpayer1_22")[
        "taxpayer1_22"
    ].transform("count")
    changed_merge_df.sort_values(
        by=["taxpayer1_22_count", "taxpayer1_22"], ascending=False, inplace=True
    )
    changed_merge_df.to_csv(
        "owners-2022.csv", quoting=csv.QUOTE_NONNUMERIC, index=False
    )


def compare_23_22():
    gdf_22 = gpd.read_file(
        os.path.join(INPUT_DIR, "city", "IPDS 2022", "det_20220000.shp")
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "taxpayer1",
            "taxpayer2",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    gdf_22["own_id"] = ""
    gdf_22["parcel_num"] = gdf_22["parcel_num"].apply(fix_parcelno)
    gdf_22.drop_duplicates(subset=["parcel_num"], inplace=True)
    gdf_22["taxpayer1"] = gdf_22["taxpayer1"].apply(clean_owner)
    gdf_22["taxpayer2"] = gdf_22["taxpayer2"].apply(clean_owner)
    gdf_22["taxpayer_address"] = (
        gdf_22["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )

    own_df = pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv"))
    own_df["taxpayer1"] = own_df["taxpayer1"].apply(clean_owner)
    own_df["taxpayer2"] = own_df["taxpayer2"].apply(clean_owner)
    own_records = own_df.to_dict(orient="records")

    own_id_map = {}
    for r in own_records:
        if not r["own_id"] or "UNID" in r["own_id"] or "UNK_" in r["own_id"]:
            continue
        if r["taxpayer1"] not in own_id_map:
            own_id_map[r["taxpayer1"]] = r["own_id"]

    df_23 = pd.read_csv(
        os.path.join(INPUT_DIR, "city", "parcels_2023.csv"), dtype={"pnum": "str"}
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "address",
            "taxpayer1",
            "taxpayer2",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]

    df_23["own_id"] = ""
    df_23["parcel_num"] = df_23["parcel_num"].apply(fix_parcelno)
    df_23.drop_duplicates(subset=["parcel_num"], inplace=True)
    df_23["address"] = (
        df_23["address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_23["taxpayer_address"] = (
        df_23["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_23["taxpayer1"] = df_23["taxpayer1"].apply(clean_owner)
    df_23["taxpayer2"] = df_23["taxpayer2"].apply(clean_owner)

    to_merge_23_df = (
        df_23[
            [
                "parcel_num",
                "address",
                "taxpayer1",
                "taxpayer2",
                "own_id",
                "taxpayer_address",
                "taxpayer_city",
            ]
        ]
        .rename(
            columns={
                "taxpayer1": "taxpayer1_23",
                "taxpayer2": "taxpayer2_23",
                "own_id": "own_id_23",
                "taxpayer_address": "taxpayer_address_23",
                "taxpayer_city": "taxpayer_city_23",
            }
        )
        .sort_values(by=["parcel_num", "own_id_23"], ascending=True)
        .drop_duplicates(subset=["parcel_num"])
    )
    merge_df = to_merge_23_df.merge(gdf_22, on="parcel_num", how="left")

    # Don't override own_id if exists already
    def pull_own_id(d):
        if d["taxpayer1_23"] in own_id_map:
            return own_id_map[d["taxpayer1_23"]]
        if d["taxpayer2_23"]:
            # Try 2 if 1 not a match
            return own_id_map.get(d["taxpayer2_23"])
        return ""

    merge_df["own_id_23"] = merge_df.apply(
        pull_own_id,
        axis=1,
    )
    merge_df["taxpayer1_23"] = merge_df["taxpayer1_23"].fillna("")
    merge_df["taxpayer_address_23"] = merge_df["taxpayer_address_23"].fillna("")

    changed_merge_df = merge_df[
        (merge_df["taxpayer1"] != merge_df["taxpayer1_23"])
        & (~merge_df["taxpayer1_23"].str.contains(DETROIT_RE))
        & (merge_df["taxpayer1_23"] != "HUD")
        & (merge_df["address"] != merge_df["taxpayer_address_23"])
        & (merge_df["own_id_23"] == "")
    ]
    changed_merge_df["taxpayer1_23_count"] = changed_merge_df.groupby("taxpayer1_23")[
        "taxpayer1_23"
    ].transform("count")
    changed_merge_df.sort_values(
        by=["taxpayer1_23_count", "taxpayer1_23"], ascending=False, inplace=True
    )
    changed_merge_df.to_csv(
        "owners-2023.csv", quoting=csv.QUOTE_NONNUMERIC, index=False
    )


def compare_24_23():
    df_23 = pd.read_csv(
        os.path.join(INPUT_DIR, "city", "parcels_2023.csv"), dtype={"pnum": "str"}
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "taxpayer1",
            "taxpayer2",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]

    df_23["own_id"] = ""
    df_23["parcel_num"] = df_23["parcel_num"].apply(fix_parcelno)
    df_23.drop_duplicates(subset=["parcel_num"], inplace=True)
    df_23["taxpayer_address"] = (
        df_23["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_23["taxpayer1"] = df_23["taxpayer1"].apply(clean_owner)
    df_23["taxpayer2"] = df_23["taxpayer2"].apply(clean_owner)

    own_df = pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv"))
    own_df["taxpayer1"] = own_df["taxpayer1"].apply(clean_owner)
    own_df["taxpayer2"] = own_df["taxpayer2"].apply(clean_owner)
    own_records = own_df.to_dict(orient="records")

    own_id_map = {}
    for r in own_records:
        if not r["own_id"] or "UNID" in r["own_id"] or "UNK_" in r["own_id"]:
            continue
        if r["taxpayer1"] not in own_id_map:
            own_id_map[r["taxpayer1"]] = r["own_id"]

    df_24 = pd.read_csv(
        os.path.join(INPUT_DIR, "city", "parcels_2024.csv"),
        dtype={"parcel_number": "str"},
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "address",
            "taxpayer1",
            "taxpayer2",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    df_24["own_id"] = ""
    df_24["parcel_num"] = df_24["parcel_num"].apply(fix_parcelno)
    df_24.drop_duplicates(subset=["parcel_num"], inplace=True)
    df_24["address"] = (
        df_24["address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_24["taxpayer_address"] = (
        df_24["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    df_24["taxpayer1"] = df_24["taxpayer1"].apply(clean_owner)
    df_24["taxpayer2"] = df_24["taxpayer2"].apply(clean_owner)

    to_merge_24_df = (
        df_24[
            [
                "parcel_num",
                "address",
                "taxpayer1",
                "taxpayer2",
                "own_id",
                "taxpayer_address",
                "taxpayer_city",
            ]
        ]
        .rename(
            columns={
                "taxpayer1": "taxpayer1_24",
                "taxpayer2": "taxpayer2_24",
                "own_id": "own_id_24",
                "taxpayer_address": "taxpayer_address_24",
                "taxpayer_city": "taxpayer_city_24",
            }
        )
        .sort_values(by=["parcel_num", "own_id_24"], ascending=True)
        .drop_duplicates(subset=["parcel_num"])
    )
    merge_df = to_merge_24_df.merge(df_23, on="parcel_num", how="left")

    # Don't override own_id if exists already
    def pull_own_id(d):
        if d["taxpayer1_24"] in own_id_map:
            return own_id_map[d["taxpayer1_24"]]
        if d["taxpayer2_24"]:
            # Try 2 if 1 not a match
            return own_id_map.get(d["taxpayer2_24"])
        return ""

    merge_df["own_id_24"] = merge_df.apply(
        pull_own_id,
        axis=1,
    )
    merge_df["taxpayer1_24"] = merge_df["taxpayer1_24"].fillna("")
    merge_df["taxpayer_address_24"] = merge_df["taxpayer_address_24"].fillna("")

    changed_merge_df = merge_df[
        (merge_df["taxpayer1"] != merge_df["taxpayer1_24"])
        & (~merge_df["taxpayer1_24"].str.contains(DETROIT_RE))
        & (merge_df["taxpayer1_24"] != "HUD")
        & (merge_df["address"] != merge_df["taxpayer_address_24"])
        & (merge_df["own_id_24"] == "")
    ]
    changed_merge_df["taxpayer1_24_count"] = changed_merge_df.groupby("taxpayer1_24")[
        "taxpayer1_24"
    ].transform("count")
    changed_merge_df.sort_values(
        by=["taxpayer1_24_count", "taxpayer1_24"], ascending=False, inplace=True
    )
    changed_merge_df.to_csv(
        "owners-2024.csv", quoting=csv.QUOTE_NONNUMERIC, index=False
    )


def main():
    compare_21_20()
    compare_22_21()
    compare_23_22()
    compare_24_23()


if __name__ == "__main__":
    main()
