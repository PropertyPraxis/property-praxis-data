# Pull all items where taxpayer address doesn't match parcel address
"""
Steps 
- pull previous year, get all PINs, taxpayers, own_id values
- create a dictionary mapping of original cleaned owners to own_ids to use later
- compare sale dates to only get items that have changed since the last latest sale date
- Create subset of items that have changed since that latest date
- Pull forward from own_id_map existing matching owners when changed
- For new and changed, filter to institutional where taxpayer doesn't match parcel
- Once you have new and changed, write to a file
- For file being written, create manual crosswalk, then use
"""

"""
Updated steps
- pull full parcel data, then merge to own_id data
- after merging in own_id to both sets of parcel data, then compare sales
- 
"""
import datetime
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
    "parcelno": "parcel_num",
    "taxpayer_1": "taxpayer1",
    "taxpayer_2": "taxpayer2",
    "saledate": "sale_date",
    "propaddr": "address",
    "taxpayer_s": "taxpayer_address",
    "taxpayer_c": "taxpayer_city",
    "tpaddr": "taxpayer_address",
    "tpcity": "taxpayer_city",
}
SPACE_RE = r"\s+"
CLEAN_RE = r"(\.|,)"
ADDR_SUFFIX_RE = r" (DR|DRIVE|AVE|AVENUE|ST|STREET|BLVD|BOULEVARD|FWY|FREEWAY)\.?$"


def clean_owner(owner):
    return re.sub(r"[^A-Za-z ]+", "", owner)


def clean_dates(date_str):
    if not isinstance(date_str, str):
        return date_str
    if "1900" in date_str:
        return np.nan
    try:
        return pd.to_datetime(
            date_str, format=("%m/%d/%Y" if ("/" in date_str) else "%Y-%m-%d")
        )
    except Exception:
        return np.nan


def fix_parcelno(parcelno):
    if not ("." in parcelno or "-" in parcelno):
        parcelno = parcelno + "."
    if len(parcelno.split(".")[0]) == 7 and "." in parcelno:
        parcelno = "0" + parcelno
    return parcelno


def main():
    gdf_20 = gpd.read_file(
        f"zip://{os.path.join(INPUT_DIR, 'praxis_shapefiles', 'praxis2020.shp.zip')}"
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            "taxpayer1",
            "taxpayer2",
            "sale_date",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    # TODO: Base one needs to pull from all parcels, get sales generally
    own_df_20 = pd.read_csv(
        os.path.join(INPUT_DIR, "praxis_csvs", "PPlusFinal_2020_edit.csv"),
        dtype={"parcel_num": "str", "parcelno": "str"},
    ).rename(columns=COL_MAP)[
        [
            "parcel_num",
            # "taxpayer1",
            # "taxpayer2",
            "own_id",
            # "sale_date",
            # "taxpayer_address",
            # "taxpayer_city",
        ]
    ]
    gdf_20["parcel_num"] = gdf_20["parcel_num"].apply(fix_parcelno)
    own_df_20["parcel_num"] = own_df_20["parcel_num"].apply(fix_parcelno)
    df_20 = gdf_20.merge(own_df_20, on="parcel_num", how="left")
    df_20["sale_date"] = df_20["sale_date"].apply(clean_dates).dt.date
    # df_20["address"] = (
    #     df_20["address"]
    #     .str.replace(SPACE_RE, " ", regex=True)
    #     .str.strip()
    #     .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    # )
    df_20["taxpayer_address"] = (
        df_20["taxpayer_address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )
    records_20 = df_20.to_dict(orient="records")
    # TODO: Include taxpayer2?
    # TODO: Clean this to match
    own_id_map = {r["taxpayer1"]: r["own_id"] for r in records_20}

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
            "sale_date",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ]
    df_21["parcel_num"] = df_21["parcel_num"].apply(fix_parcelno)
    df_21["sale_date_21"] = df_21["sale_date"].apply(clean_dates).dt.date
    df_21["address"] = (
        df_21["address"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )

    to_merge_21_df = df_21[
        [
            "parcel_num",
            "address",
            "taxpayer1",
            "own_id",
            "sale_date_21",
            "taxpayer_address",
            "taxpayer_city",
        ]
    ].rename(
        columns={
            "taxpayer1": "taxpayer1_21",
            "own_id": "own_id_21",
            "taxpayer_address": "taxpayer_address_21",
            "taxpayer_city": "taxpayer_city_21",
        }
    )
    merge_df = to_merge_21_df.merge(df_20, on="parcel_num", how="left")
    # Don't override own_id if exists already
    merge_df["own_id_21"] = merge_df.apply(
        lambda d: own_id_map.get(d["taxpayer1_21"])
        if not d["own_id_21"]
        else d["own_id_21"],
        axis=1,
    )
    merge_df["taxpayer_address_21"] = merge_df["taxpayer_address_21"] = (
        merge_df["taxpayer_address_21"]
        .str.replace(SPACE_RE, " ", regex=True)
        .str.strip()
        .str.replace(CLEAN_RE, "", regex=True)
        .str.replace(ADDR_SUFFIX_RE, "", regex=True)
    )

    print(merge_df.shape)
    # TODO: Assuming we can count on sale date here?
    changed_merge_df = merge_df[
        (
            ~pd.isna(merge_df["sale_date"])
            & (merge_df["sale_date_21"] > merge_df["sale_date"])
        )
        # TODO: Disable this condition when we start from a full parcel list in previous
        # | (merge_df["sale_date_21"] > datetime.date(2020, 1, 1))
    ]
    print(changed_merge_df.shape)
    print(
        changed_merge_df[
            changed_merge_df["address"] != changed_merge_df["taxpayer_address_21"]
        ].shape
    )
    changed_merge_df[
        changed_merge_df["address"] != changed_merge_df["taxpayer_address_21"]
    ].to_csv("owners-2021.csv", index=False)


if __name__ == "__main__":
    main()
