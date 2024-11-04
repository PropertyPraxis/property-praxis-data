S3_BUCKET = property-praxis-data
YEARS = 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024

all: $(foreach year,$(YEARS),tiles/parcels-$(year)/ tiles/parcels-centroids-$(year)/)

.PHONY: data
data: input/praxis_csvs/ input/praxis_shapefiles/ input/zipcodes.geojson
	poetry run python scripts/clean_files.py

.PRECIOUS: tiles/%/
tiles/%/: tiles/%.mbtiles
	tile-join --no-tile-size-limit --force -e $@ $<

# TODO: Switch at 14 instead?
.PRECIOUS: tiles/parcels-centroids-%.mbtiles
tiles/parcels-centroids-%.mbtiles: data/parcels-centroids-%.geojson
	tippecanoe \
	--simplification=10 \
	--simplify-only-low-zooms \
	--minimum-zoom=8 \
	--maximum-zoom=14 \
	--no-tile-stats \
	--detect-shared-borders \
	--coalesce-smallest-as-needed \
	--attribute-type=parcelno:string \
	--attribute-type=zipcode_sj:string \
	--use-attribute-for-id=prop_id \
	--force \
	-L parcels:$< -o $@

.PRECIOUS: tiles/parcels-%.mbtiles
tiles/parcels-%.mbtiles: data/parcels-%.geojson
	tippecanoe \
	--simplification=10 \
	--simplify-only-low-zooms \
	--minimum-zoom=13 \
	--maximum-zoom=14 \
	--no-tile-stats \
	--detect-shared-borders \
	--grid-low-zooms \
	--coalesce-smallest-as-needed \
	--attribute-type=parcelno:string \
	--attribute-type=zipcode_sj:string \
	--use-attribute-for-id=prop_id \
	--force \
	-L parcels:$< -o $@

input/praxis_csvs/PPlusFinal_2024.csv: input/own-id-map.csv
	poetry run python scripts/clean_2024.py

input/praxis_csvs/PPlusFinal_2023.csv: input/own-id-map.csv
	poetry run python scripts/clean_2023.py

input/praxis_csvs/PPlusFinal_2022.csv: input/own-id-map.csv
	poetry run python scripts/clean_2022.py

input/praxis_csvs/PPlusFinal_2021.csv: input/own-id-map.csv
	poetry run python scripts/clean_2021.py

input/own-id-map.csv:
	poetry run python scripts/own_id_map.py

# TODO: fix projection here to avoid handling in geopandas
input/zipcodes.geojson:
	wget -O $@ "https://opendata.arcgis.com/datasets/f6273f93db1b4f57b7091ef1f43271e7_0.geojson"

input/%:
	aws s3 cp s3://$(S3_BUCKET)/$* ./data/$* --recursive
