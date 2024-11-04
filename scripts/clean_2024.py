import os
import re

import geopandas as gpd
import pandas as pd

COL_MAP = {
    "pnum": "parcel_num",
    "parcelno": "parcel_num",
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
}

INPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "input"
)


def clean_owner(owner):
    if not isinstance(owner, str):
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9 ]+", "", owner)).strip()


if __name__ == "__main__":
    parcel_gdf = gpd.read_file(os.path.join(INPUT_DIR, "city", "parcels_2024.geojson"))

    own_id_map = {}
    for record in pd.read_csv(os.path.join(INPUT_DIR, "own-id-map.csv")).to_dict(
        orient="records"
    ):
        own_id_map[clean_owner(record["taxpayer1"])] = record["own_id"]

    parcel_gdf["own_id"] = (
        parcel_gdf["taxpayer_1"]
        .apply(clean_owner)
        .map(own_id_map)
        .fillna(parcel_gdf["taxpayer_2"].apply(clean_owner).map(own_id_map))
    )
    parcel_gdf["propstr"] = ""

    parcel_gdf = parcel_gdf[~pd.isnull(parcel_gdf["own_id"])]
    parcel_gdf["sale_date"] = parcel_gdf["sale_date"].dt.strftime("%Y-%m-%d")
    parcel_gdf.to_file(os.path.join(INPUT_DIR, "praxis_shapefiles", "praxis2024.shp"))
    parcel_gdf.drop(labels=["geometry"], axis=1).to_csv(
        os.path.join(INPUT_DIR, "praxis_csvs", "PPlusFinal_2024_edit.csv"), index=False
    )
