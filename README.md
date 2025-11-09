# Property Praxis Data

Code for building the Property Praxis database

## Build locally

```
poetry install --no-root
make data
make tiles
```

## Steps to rebuild

- Download a parcel file from the city's data portal
- Create a list of parcels that have changed ownership and aren't currently coded by updating and running `scripts/identify_ownership.py`
- Once the coding is done for the year, save it to `input/own-id-$YEAR.csv`
- Update and run `scripts/own_id_map.py` to create a new mapping of all taxpayers that have been coded to speculator names
- Create `clean_$YEAR.py` and run it to create new files
- Backup any created files to the S3 bucket
- Update `scripts/clean_files.py` with any changes from the latest data
- With a fresh database, run `sql/schema.sql` to load the database schema
- With the schema loaded, run `python scripts/clean_files.py` to merge and reload the database
- Run `make tiles/$YEAR/` to regenerate the vector tiles and then deploy them to S3
- Run `pg_dump` and `pg_restore` with the `--clean` flag to overwrite the existing database with new records
