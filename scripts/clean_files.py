import os
import re

import geopandas as gpd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

db = create_engine(
    os.getenv("DATABASE_URL", "postgresql://postgres:postgres@0.0.0.0:35432/postgres")
)

INPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "input"
)
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
)

BASE_COLS = [
    "taxpayer1",
    "taxpayer2",
    "own_id",
    "tpaddr",
    "tpcity",
    "tpstate",
    "tpzip",
    "parcelno",
    "propaddr",
    "propno",
    "propdir",
    "propstr",
    "propzip",
    "year",
    # adding these field provides no additional records
    "taxstatus",
    "saledate",
    "saleprice",
    "totsqft",
    "totacres",
    "cityrbuilt",
    "resyrbuilt",
]

COL_MAP = {
    "id_old": "old_id",
    "OBJECTID": "id",
    "taxpayer1": "taxpayer",
    "taxpayer 1": "taxpayer",  # TODO: fix this
    "taxpayer_1": "taxpayer",
    "taxpayer 2": "taxpayer2",
    "taxpayer_2": "taxpayer2",
    "cibyrbuilt": "cityrbuilt",
    "parcelnumber": "parcelno",
    "parcel_num": "parcelno",
    "parcelnum": "parcelno",
    "address": "propaddr",
    "zipcode": "propzip",
    "taxpayerstreet": "tpaddr",
    "taxpayer_s": "tpaddr",
    "taxpayercity": "tpcity",
    "taxpayer_c": "tpcity",
    "taxpayerzip": "tpzip",
    "taxpayer_z": "tpzip",
    "taxpayerstate": "tpstate",
    "taxpayer_3": "tpstate",
    "yearbuilt": "resyrbuilt",
    "totalsquarefootage": "totsqft",
    "tax_status": "taxstatus",
    "sale_date": "saledate",
    "sale_price": "saleprice",
    "total_squa": "totsqft",
    "total_acre": "totacres",
    "year_built": "resyrbuilt"
    # TODO: Typo here, total something?
    # "taxpayerstate": "totacres",
}


def clean_owner(owner):
    if not isinstance(owner, str):
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9 ]+", "", owner)).strip()


def own_group(count):
    if count > 9 and count <= 20:
        return 1
    if count > 20 and count <= 100:
        return 2
    if count > 100 and count <= 200:
        return 3
    if count > 200 and count <= 500:
        return 4
    if count > 500 and count <= 1000:
        return 5
    if count > 1000 and count <= 1500:
        return 6
    if count > 1500:
        return 7
    return 0


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


def fix_zipcodes(shp_df, zip_df):
    zip_df = zip_df[["geometry", "zipcode"]].set_geometry("geometry").to_crs(4326)

    # Add a temporary ID column
    shp_df["temp_id"] = range(1, len(shp_df) + 1)
    centroid_df = shp_df.copy()
    centroid_df["geometry"] = centroid_df.to_crs(3857).centroid.to_crs(4326)
    centroid_df = centroid_df.set_geometry("geometry")
    within_df = gpd.sjoin(
        centroid_df,
        zip_df,
        predicate="within",
    )
    na_df = within_df[within_df["zipcode"].isna()].drop(columns=["zipcode"])
    within_df = within_df.dropna(subset=["zipcode"]).to_crs(4326)

    na_df = (
        gpd.GeoDataFrame(na_df)
        .rename(columns={0: "geometry", "index_right": "ir"})
        .set_geometry("geometry")
        .to_crs(4326)
    )
    na_df = gpd.sjoin_nearest(na_df, zip_df)

    within_df = within_df.drop(columns="geometry")
    na_df = na_df.drop(columns="geometry")

    join_df = pd.concat([within_df, na_df]).sort_values(by="temp_id")[
        ["temp_id", "zipcode"]
    ]
    shp_df = gpd.GeoDataFrame(
        pd.merge(shp_df, join_df, on=["temp_id"], how="left")
    ).rename(columns={"zipcode": "zipcode_sj"})

    return shp_df.dropna(subset=["zipcode_sj"])


def add_propno_if_missing(df):
    if "propno" not in df.columns:
        df["propno"] = df["propstr"].apply(
            lambda val: pd.to_numeric(str(val).split()[0], errors="coerce")
        )
    return df


def clean_csv_df(csv_filename):
    year_str = re.search(r"\d{4}", csv_filename)[0]
    df = pd.read_csv(
        csv_filename,
        dtype={
            "parcelnumber": "str",
            "parcel_num": "str",
            "parcelnum": "str",
            "propno": "str",
            "propstr": "str",
        },
    )
    df = df.rename(columns=COL_MAP)
    df = add_propno_if_missing(df)
    df["parcelno"] = df["parcelno"].apply(fix_parcelno)
    if "cityrbuilt" not in df.columns:
        df["cityrbuilt"] = np.nan

    df["year"] = int(year_str)
    return df


def clean_shp_df(shp_filename, zip_df, parcel_prop_df):
    read_filename = shp_filename
    if read_filename.endswith(".zip"):
        read_filename = "zip://" + read_filename
    shp_df = gpd.read_file(read_filename)
    shp_df = shp_df.rename(columns=COL_MAP)
    # TODO: set_geometry somewhere
    shp_df = fix_zipcodes(shp_df, zip_df)

    shp_df = shp_df.loc[
        ~shp_df["parcelno"].isna(), ["parcelno", "propaddr", "zipcode_sj", "geometry"]
    ]
    shp_df["tmp_id"] = shp_df["parcelno"].astype(str) + shp_df["propaddr"].astype(str)

    dup_ids = shp_df.loc[shp_df.duplicated("tmp_id"), "tmp_id"].unique()
    dup_shp = shp_df.loc[shp_df["tmp_id"].isin(dup_ids)]

    # Aggregating duplicated rows
    dup_shp = (
        dup_shp.groupby(["parcelno", "propaddr"])
        .agg(
            geometry=("geometry", lambda g: g.unary_union),
            geom_agg_count=("geometry", "size"),
            zipcode_sj=("zipcode_sj", lambda x: ", ".join(x.unique())),
        )
        .reset_index()
    )

    uni_shp = shp_df.loc[~shp_df["tmp_id"].isin(dup_ids)]
    shp_df = pd.concat([dup_shp, uni_shp])
    geom_df = pd.merge(parcel_prop_df, shp_df, on=["parcelno", "propaddr"])

    year_str = re.search(r"\d{4}", shp_filename)[0]
    geom_df["year"] = int(year_str)

    geom_df = geom_df.drop_duplicates()
    geom_df = geom_df.set_geometry("geometry")
    geom_df.crs = "EPSG:4326"
    return geom_df


if __name__ == "__main__":
    own_id_map = {}
    for record in pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv")).to_dict(
        orient="records"
    ):
        own_id_map[clean_owner(record["taxpayer1"])] = record["own_id"]

    csv_df_list = []
    years = []
    for csv_filename in os.listdir(os.path.join(INPUT_DIR, "praxis_csvs")):
        if "Final" not in csv_filename:
            continue
        year_str = re.search(r"\d{4}", csv_filename)[0]
        years.append(int(year_str))
        print(csv_filename)
        csv_df_list.append(
            clean_csv_df(os.path.join(INPUT_DIR, "praxis_csvs", csv_filename))
        )
    # TODO: Load others post 2020 here
    full_df = pd.concat(csv_df_list, ignore_index=True).drop_duplicates()

    full_df["own_id"] = (
        full_df["taxpayer"]
        .apply(clean_owner)
        .map(own_id_map)
        .fillna(full_df["taxpayer2"].apply(clean_owner).map(own_id_map))
    )
    full_df = full_df[~pd.isnull(full_df["own_id"])]

    parcel_df = full_df.drop_duplicates(
        subset=["parcelno", "propaddr", "propno"]
    ).rename(columns={"geom": "geom_"})

    print("reading zip")
    zip_df = gpd.read_file(os.path.join(INPUT_DIR, "zipcodes.geojson"))

    geom_df_list = []
    for shp_filename in os.listdir(os.path.join(INPUT_DIR, "praxis_shapefiles")):
        if shp_filename.endswith(".zip") or shp_filename.endswith(".shp"):
            print(shp_filename)
            geom_df_list.append(
                clean_shp_df(
                    os.path.join(INPUT_DIR, "praxis_shapefiles", shp_filename),
                    zip_df,
                    parcel_df,
                )
            )

    parcel_prop_df = pd.concat(geom_df_list).drop_duplicates(
        subset=["parcelno", "year"]
    )

    prop_df = (
        parcel_prop_df.drop_duplicates(subset=["parcelno", "propaddr"])
        .reset_index()
        .rename(columns={"index": "_old_index"})
        .reset_index()
        .rename(columns={"index": "prop_id"})
        .rename(columns=COL_MAP)
    )

    prop_db_df = prop_df[
        [
            "prop_id",
            "propno",
            "parcelno",
            "propaddr",
            "propdir",
            "propstr",
            "propzip",
            "zipcode_sj",
        ]
    ]

    parcel_prop_df = (
        parcel_prop_df.reset_index()
        .rename(columns={"index": "_old_index"})
        .reset_index()
        .rename(columns={"index": "parprop_id"})
        .rename(columns=COL_MAP)
    )

    parcel_prop_df = pd.merge(
        parcel_prop_df,
        prop_df[["parcelno", "propaddr", "prop_id"]],
        on=["parcelno", "propaddr"],
        how="left",
    )

    own_group_df = (
        full_df.groupby(["year", "own_id"])[["id"]]
        .count()
        .reset_index()
        .rename(columns={"id": "own_count"})
    )
    own_group_df["own_group"] = own_group_df["own_count"].apply(own_group)

    parcel_prop_df = pd.merge(
        parcel_prop_df,
        own_group_df[["year", "own_id", "own_count", "own_group"]],
        on=["year", "own_id"],
        how="left",
    )

    parcel_prop_df = gpd.GeoDataFrame(
        parcel_prop_df[
            [
                "parprop_id",
                "prop_id",
                "parcelno",
                "propaddr",
                "year",
                "own_id",
                "own_group",
                "own_count",
                "geometry",
                "zipcode_sj",
            ]
        ].rename(columns={"geometry": "geom"}),
        crs="EPSG:4326",
        geometry="geom",
    )

    for year in parcel_prop_df["year"].unique():
        parcel_prop_year = gpd.GeoDataFrame(
            parcel_prop_df.loc[
                (parcel_prop_df["year"] == year) & (parcel_prop_df["own_group"] > 0)
            ][
                [
                    "prop_id",
                    "parcelno",
                    "propaddr",
                    "year",
                    "own_id",
                    "own_group",
                    "own_count",
                    "zipcode_sj",
                    "geom",
                ]
            ],
            crs="EPSG:4326",
            geometry="geom",
        )
        parcel_prop_year.to_file(os.path.join(DATA_DIR, f"parcels-{year}.geojson"))
    parcel_prop_df["centroid"] = parcel_prop_df.centroid
    parcel_prop_df = gpd.GeoDataFrame(
        parcel_prop_df[
            [
                "parprop_id",
                "prop_id",
                "parcelno",
                "propaddr",
                "own_id",
                "own_group",
                "own_count",
                "year",
                "centroid",
                "zipcode_sj",
            ]
        ],
        crs="EPSG:4326",
        geometry="centroid",
    )

    for year in parcel_prop_df["year"].unique():
        parcel_prop_year = gpd.GeoDataFrame(
            parcel_prop_df.loc[
                (parcel_prop_df["year"] == year) & (parcel_prop_df["own_group"] > 0)
            ][
                [
                    "prop_id",
                    "parcelno",
                    "propaddr",
                    "year",
                    "own_id",
                    "own_group",
                    "own_count",
                    "zipcode_sj",
                    "centroid",
                ]
            ],
            crs="EPSG:4326",
            geometry="centroid",
        )
        parcel_prop_year.to_file(
            os.path.join(DATA_DIR, f"parcels-centroids-{year}.geojson")
        )

    parcel_prop_df.drop(columns=["own_group", "own_id", "own_count"], inplace=True)

    print("writing zips_geom")
    zip_df[["zipcode", "geometry"]].to_postgis("zips_geom", if_exists="append", con=db)
    print("wrote zips_geom")

    print("writing property")
    prop_db_df.to_sql("property", if_exists="append", con=db, index=False)
    print("wrote property")

    print("writing parcel_property_geom")
    parcel_prop_df.to_postgis(
        "parcel_property_geom", if_exists="append", con=db, index=False
    )
    print("wrote parcel_property_geom")

    prop_merge_df = prop_df[["parcelno", "propaddr", "prop_id"]]
    full_df = pd.merge(full_df, prop_merge_df, on=["parcelno", "propaddr"], how="left")

    owntax_df = (
        full_df[["own_id", "taxpayer"]]
        .drop_duplicates()
        .reset_index()
        .rename(columns={"index": "owntax_id"})
    )
    print("writing owner_taxpayer")
    owntax_df.to_sql("owner_taxpayer", if_exists="append", con=db, index=False)
    print("wrote owner_taxpayer")
    full_df = pd.merge(full_df, owntax_df, on=["own_id", "taxpayer"], how="left")

    taxpayer_df = (
        full_df[
            [
                "owntax_id",
                "taxpayer2",
                "tpaddr",
                "tpcity",
                "tpstate",
                "tpzip",
                "taxstatus",
            ]
        ]
        .drop_duplicates()
        .reset_index()
        .rename(columns={"index": "tp_id"})
    )
    print("writing taxpayer")
    taxpayer_df.to_sql("taxpayer", if_exists="append", con=db, index=False)
    print("wrote taxpayer")
    full_df = pd.merge(
        full_df,
        taxpayer_df,
        on=[
            "owntax_id",
            "taxpayer2",
            "tpaddr",
            "tpcity",
            "tpstate",
            "tpzip",
            "taxstatus",
        ],
        how="left",
    )

    taxpayer_property_df = (
        full_df[["tp_id", "prop_id"]]
        .drop_duplicates()
        .reset_index()
        .rename(columns={"index": "taxparprop_id"})
    )
    print("writing taxpayer_property")
    taxpayer_property_df.to_sql(
        "taxpayer_property", if_exists="append", con=db, index=False
    )
    print("wrote taxpayer_property")
    full_df = pd.merge(
        full_df, taxpayer_property_df, on=["tp_id", "prop_id"], how="left"
    )

    year_df = full_df[
        [
            "taxparprop_id",
            "year",
            "saledate",
            "saleprice",
            "totsqft",
            "totacres",
            "cityrbuilt",
            "resyrbuilt",
        ]
    ].drop_duplicates(subset=["taxparprop_id", "year"])

    year_df["saledate"] = year_df["saledate"].apply(clean_dates).dt.date
    print("writing year")
    year_df.to_sql("year", if_exists="append", con=db, index=False)
