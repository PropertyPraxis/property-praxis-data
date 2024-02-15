import os

import geopandas as gpd
import numpy as np
import pandas as pd

TMP_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmpdata"
)
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
)


if __name__ == "__main__":
    cur_df = pd.read_csv(os.path.join(TMP_DATA_DIR, "cur_data.csv"))
    new_df = pd.read_csv(os.path.join(TMP_DATA_DIR, "new_2021_with_tp_address.csv"))
    new_ownid_df = (
        pd.read_csv(
            os.path.join(TMP_DATA_DIR, "NEW_2021_AKERS012622_117.csv"),
            names=["taxpayer_1", "n", "own_id1", "own_id2"],
            skiprows=1,
        )
        .melt(
            id_vars=["taxpayer_1", "n"],
            value_vars=["own_id1", "own_id2"],
            var_name="variable",
            value_name="own_id",
        )
        .drop_duplicates(subset=["taxpayer_1", "own_id"])
    )

    new_2021_long_tp = (
        new_ownid_df.groupby("taxpayer_1")["own_id"]
        .unique()
        .apply(lambda x: x[0] if pd.notna(x[0]) else np.nan)
        .reset_index()
    )
    new_2021_long_tp2 = new_ownid_df[
        ~new_ownid_df["taxpayer_1"].isin(new_2021_long_tp["taxpayer_1"])
    ].drop_duplicates()

    own_id_2021 = pd.concat([new_2021_long_tp, new_2021_long_tp2], ignore_index=True)

    # Add new columns 'inc_years' and 'most_recent_year'
    own_id_2021["inc_years"] = "2021"
    own_id_2021["most_recent_year"] = 2021

    # Concatenate DataFrames cur_2021 and own_id_2021
    all_years = pd.concat(
        [
            cur_df.drop(columns=["n_years"]).rename(
                columns={"taxpayer1": "taxpayer_1"}
            ),
            own_id_2021,
        ],
        ignore_index=True,
    )
    # all_years.to_csv("testing.csv", index=False)

    parcel_df = gpd.read_file(os.path.join(TMP_DATA_DIR, "2021_parcels", "Parcels.shp"))
    parcel_merge_df = pd.merge(parcel_df, all_years, on=["taxpayer_1"], how="left")
    parcel_merge_df = gpd.GeoDataFrame(parcel_merge_df)
    parcel_merge_df["propstr"] = parcel_merge_df["taxpayer_s"]
    parcel_merge_df["propdir"] = ""
    parcel_merge_df["propzip"] = parcel_merge_df["taxpayer_z"]
    # breakpoint()
    # TODO: Try after getting basics to work
    parcel_merge_df.to_file(
        os.path.join(DATA_DIR, "praxis_shapefiles", "praxis2021.shp")
    )
    parcel_merge_df.drop(labels=["geometry"], axis=1).to_csv(
        os.path.join(DATA_DIR, "praxis_csvs", "PPlusFinal_2021_edit.csv"), index=False
    )
    # parcel_merge_df.drop(labels=["geometry"], axis=1).to_csv(
    #     os.path.join(TMP_DATA_DIR, "parcel-data.csv"), index=False
    # )
