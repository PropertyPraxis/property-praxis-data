import csv
import os
import re

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.ops import unary_union
from sqlalchemy import create_engine

db = create_engine(
    os.getenv("DATABASE_URL", "postgresql://postgres:postgres@0.0.0.0:35432/postgres")
)

YEARS = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

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
    "parcel_number": "parcelno",
    "address": "propaddr",
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


ZIP_COL_MAP = {
    "zipcode": "propzip",
    "zip_code": "propzip",
}

EXCLUDE_TAXPAYER_LIST = ["HUD"]
EXCLUDE_RE = r"LAND BANK|CITY OF DETROIT|DETROIT PARKS|BRIDGE AUTHORITY|MDOT|DEPARTMENT OF|DEPT OF|UNK_|UNIDENTIFIED|UNKNOWN|TRUST|HENRY FORD|UAT|UAW|DTE|FCA|WAYNE COUNTY|NON\-PROFIT|TAXPAYER|RECOVERYPARK|RECOVERY PARK|VHS HARPER|HARPER\-HUTZEL|POPE FRANCIS|DETROIT MERCY|CATHEDRAL|PARISH|PERFECTING CHURCH"  # noqa


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
    if "propzip" not in df.columns:
        df = df.rename(columns=ZIP_COL_MAP)

    df["year"] = int(year_str)
    return df


def add_zipcode_with_most_overlap(parcels_gdf, zips_gdf):
    joined_gdf = gpd.sjoin(parcels_gdf, zips_gdf, how="left", op="intersects")

    joined_gdf["overlap_area"] = joined_gdf.geometry.intersection(
        joined_gdf.geometry
    ).area
    joined_gdf["original_index"] = joined_gdf.index

    result = joined_gdf.loc[
        joined_gdf.groupby("original_index")["overlap_area"].idxmax()
    ][["original_index", "zipcode"]]

    parcels_gdf = parcels_gdf.merge(
        result, left_index=True, right_on="original_index", how="left"
    )

    parcels_gdf.drop(columns=["original_index"], inplace=True)
    return parcels_gdf


def clean_shp_df(shp_filename, zip_df, parcel_df):
    read_filename = shp_filename
    if read_filename.endswith(".zip"):
        read_filename = "zip://" + read_filename
    shp_df = gpd.read_file(read_filename)
    shp_df = gpd.GeoDataFrame(
        shp_df.rename(columns=COL_MAP)[["parcelno", "geometry"]],
        geometry="geometry",
        crs=shp_df.crs,
    )

    year = int(re.search(r"\d{4}", shp_filename)[0])
    parcel_df = parcel_df.loc[parcel_df["year"] == year]

    geom_parcel_gdf = shp_df.loc[
        shp_df["parcelno"].notna()
        & shp_df["parcelno"].isin(parcel_df["parcelno"].dropna())
    ].to_crs("EPSG:3857")
    geom_parcel_gdf = add_zipcode_with_most_overlap(
        geom_parcel_gdf, zip_df[["zipcode", "geometry"]].to_crs("EPSG:3857")
    )
    geom_parcel_gdf = geom_parcel_gdf.rename(columns={"zipcode": "propzip"})

    parcel_gdf = (
        geom_parcel_gdf.sort_values(["parcelno"])
        .groupby(["parcelno"], as_index=False)
        .agg({"geometry": lambda geoms: unary_union(geoms), "propzip": "first"})
    )

    parcel_gdf = parcel_gdf.drop_duplicates()
    parcel_gdf["year"] = year

    return gpd.GeoDataFrame(parcel_gdf, geometry="geometry", crs="EPSG:3857").to_crs(
        4326
    )


def clean_own_id(own_id):
    return re.sub(r"\s+", " ", own_id.upper()).strip()


def get_own_id_map():
    own_id_map = {}
    own_id_df = pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv"))
    for record in own_id_df.to_dict(orient="records"):
        own_id_map[clean_owner(record["taxpayer1"])] = record["own_id"]
        # TODO: add taxpayer2?
    return own_id_map


if __name__ == "__main__":
    own_id_map = get_own_id_map()

    csv_df_list = []
    for year in YEARS:
        print(f"CSV: {year}")
        csv_df_list.append(
            clean_csv_df(
                os.path.join(INPUT_DIR, "praxis_csvs", f"PPlusFinal_{year}_edit.csv")
            )
        )

    combined_df = pd.concat(csv_df_list, ignore_index=True).drop_duplicates(
        subset=["parcelno", "year"]
    )

    combined_df["own_id"] = (
        combined_df["taxpayer"]
        .apply(clean_owner)
        .map(own_id_map)
        .fillna(combined_df["taxpayer2"].apply(clean_owner).map(own_id_map))
    )
    combined_df = combined_df[~pd.isnull(combined_df["own_id"])]
    combined_df = combined_df[
        ~(
            combined_df["own_id"].str.contains(EXCLUDE_RE, regex=True)
            | combined_df["taxpayer"].str.contains(EXCLUDE_RE, regex=True)
            | combined_df["taxpayer2"].str.contains(EXCLUDE_RE, regex=True)
            | combined_df["taxpayer"].isin(EXCLUDE_TAXPAYER_LIST)
        )
    ]

    # Only retain owners for years where they have at least 10 parcels
    # TODO: Maybe revisit and instead pull any owner with at least 10 parcels any year
    full_df_list = []
    for year in YEARS:
        year_own_df = (
            combined_df[combined_df["year"] == year]
            .groupby(["own_id"])
            .size()
            .reset_index()
            .rename(columns={0: "own_count"})
        )
        min_10_owners = year_own_df[year_own_df["own_count"] >= 10]["own_id"]
        full_df_list.append(
            combined_df[
                (combined_df["year"] == year)
                & (combined_df["own_id"].isin(min_10_owners))
            ]
        )
    full_df = pd.concat(full_df_list, ignore_index=True).drop_duplicates()

    print("reading zip")
    zip_df = gpd.read_file(os.path.join(INPUT_DIR, "zipcodes.geojson"))

    geom_df_list = []
    for year in YEARS:
        if year < 2022:
            shp_filename = f"praxis{year}.shp.zip"
        else:
            shp_filename = f"praxis{year}.shp"
        print(shp_filename)
        geom_df_list.append(
            clean_shp_df(
                os.path.join(INPUT_DIR, "praxis_shapefiles", shp_filename),
                zip_df,
                full_df,
            )
        )

    parcel_prop_df = pd.concat(geom_df_list).drop_duplicates(
        subset=["parcelno", "year"]
    )

    own_group_df = (
        full_df.groupby(["year", "own_id"])
        .size()
        .reset_index()
        .rename(columns={0: "own_count"})
    )
    own_group_df["own_group"] = own_group_df["own_count"].apply(own_group)

    full_own_df = pd.merge(
        full_df,
        own_group_df[["year", "own_id", "own_count", "own_group"]],
        on=["year", "own_id"],
        how="left",
    )
    full_own_df = full_own_df.rename(columns={"propzip": "propzip2"})

    parcel_df = full_own_df.merge(parcel_prop_df, how="left")
    parcel_df = parcel_df.loc[parcel_df["own_group"] > 0]
    parcel_df.loc[parcel_df["propzip"].isnull(), "propzip"] = (
        parcel_df["propzip2"]
        .where(parcel_df["propzip2"].notnull())
        .where(parcel_df["propzip"].isnull())
        .str.split("-", n=1)
        .str[0]
    )

    parcel_df = gpd.GeoDataFrame(parcel_df, geometry="geometry", crs="EPSG:4326")
    parcel_df["feature_id"] = parcel_df.index
    parcel_df["centroid"] = parcel_df.centroid
    parcel_df = parcel_df.rename(columns={"geom": "geom_"})
    parcel_df = parcel_df.rename(columns={"geometry": "geom"})
    parcel_df["count"] = parcel_df["own_count"]

    parcel_df = gpd.GeoDataFrame(
        parcel_df[
            [
                "feature_id",
                "saledate",
                "saleprice",
                "totsqft",
                "totacres",
                "year",
                "propaddr",
                "own_id",
                "taxpayer",
                "count",  # TODO: rename own_count, would require app refactor
                "own_group",
                "parcelno",
                "propno",
                "propdir",
                "propzip",
                "propzip2",
                "resyrbuilt",
                "centroid",
                "geom",
            ]
        ],
        crs="EPSG:4326",
        geometry="geom",
    )

    parcel_df["saledate"] = parcel_df["saledate"].apply(clean_dates).dt.date
    parcel_df["resyrbuilt"] = pd.to_numeric(
        parcel_df["resyrbuilt"], errors="coerce", downcast="integer"
    ).astype("Int64")

    for year in YEARS:
        print(year)
        year_df = parcel_df.loc[parcel_df["year"] == year].rename(
            columns={"count": "own_count"}
        )
        gpd.GeoDataFrame(
            year_df[
                [
                    "feature_id",
                    "parcelno",
                    "propaddr",
                    "propzip",
                    "taxpayer",
                    "year",
                    "own_id",
                    "own_group",
                    "own_count",
                    "geom",
                ]
            ],
            crs="EPSG:4326",
            geometry="geom",
        ).to_file(os.path.join(DATA_DIR, f"parcels-{year}.geojson"))

        gpd.GeoDataFrame(
            year_df[
                [
                    "feature_id",
                    "parcelno",
                    "propaddr",
                    "year",
                    "own_id",
                    "own_group",
                    "own_count",
                    "propzip",
                    "centroid",
                ]
            ],
            crs="EPSG:4326",
            geometry="centroid",
        ).to_file(os.path.join(DATA_DIR, f"parcels-centroids-{year}.geojson"))

        year_df.drop(["geom", "centroid"], axis=1).to_csv(
            os.path.join(DATA_DIR, f"parcels-{year}.csv"),
            index=False,
            quoting=csv.QUOTE_NONNUMERIC,
        )

    # TODO: Seeing a good amount of duplicates on PIN here, but addresses different
    # Have to convert directly to WKT here to avoid SQL issues
    # TODO: Why are there null values
    parcel_df["centroid"] = parcel_df["centroid"].apply(
        lambda x: f"SRID=4326;{x.wkt}" if x else None
    )

    print("writing zips_geom")
    zip_df[["zipcode", "geometry"]].to_postgis("zips_geom", if_exists="append", con=db)
    print("wrote zips_geom")

    # breakpoint()
    print("writing parcels")
    parcel_df.to_postgis("parcels", if_exists="append", con=db)
    print("wrote parcels")

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
