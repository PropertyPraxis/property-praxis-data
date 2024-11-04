CREATE EXTENSION IF NOT EXISTS postgis;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE parcels (
    feature_id SERIAL,
    saledate DATE,
    saleprice DOUBLE PRECISION,
    totsqft DOUBLE PRECISION,
    totacres DOUBLE PRECISION,
    resyrbuilt INTEGER,
    year INTEGER,
    propaddr VARCHAR(256),
    own_id VARCHAR(256),
    taxpayer VARCHAR(256),
    count INTEGER,
    own_group INTEGER,
    parcelno VARCHAR(256),
    propno DOUBLE PRECISION,
    propdir VARCHAR(256),
    propzip VARCHAR(256),
    propzip2 VARCHAR(256),
    centroid GEOMETRY(POINT, 4326),
    geom GEOMETRY(GEOMETRY, 4326),
    CONSTRAINT parcels_pk PRIMARY KEY (feature_id, year)
);

CREATE INDEX parcels_parcelno_index ON parcels (parcelno);

CREATE INDEX parcels_year_idx ON parcels (year);

CREATE INDEX parcels_zipcode_idx ON parcels USING gin(propzip gin_trgm_ops);

CREATE INDEX parcels_spatial_idx ON parcels USING gist(centroid);

CREATE INDEX parcels_spatial_geom_idx ON parcels USING gist(geom);

CREATE MATERIALIZED VIEW owner_count AS (
    (
        SELECT
            DISTINCT p.year,
            p.own_id AS own_id,
            p.own_group AS own_group,
            p.count AS count
        FROM
            parcels AS p
        GROUP BY
            p.year,
            p.own_id,
            p.own_group,
            p.count
    )
);