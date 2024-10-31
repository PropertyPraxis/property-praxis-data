CREATE EXTENSION IF NOT EXISTS postgis;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE property (
    prop_id BIGINT PRIMARY KEY,
    propno DOUBLE PRECISION,
    parcelno VARCHAR(256),
    propaddr VARCHAR(256),
    propdir VARCHAR(256),
    propstr VARCHAR(256),
    propzip VARCHAR(256),
    zipcode_sj VARCHAR(256)
);

CREATE TABLE parcel_property_geom (
    parprop_id BIGINT PRIMARY KEY,
    prop_id BIGINT REFERENCES property (prop_id),
    parcelno VARCHAR(256),
    propaddr VARCHAR(256),
    year INTEGER,
    zipcode_sj VARCHAR(256),
    centroid GEOMETRY(GEOMETRY, 4326),
    geom GEOMETRY(GEOMETRY, 4326)
);

CREATE INDEX parcel_property_geom_year_idx ON parcel_property_geom (year);

CREATE INDEX parcel_property_geom_spatial_idx ON parcel_property_geom USING gist(centroid);

CREATE INDEX parcel_property_geometry_spatial_idx ON parcel_property_geom USING gist(geom);

CREATE TABLE zips_geom (
    objectid SERIAL PRIMARY KEY,
    zipcode VARCHAR(50),
    -- TODO: index?
    geometry GEOMETRY(GEOMETRY, 4326) -- TODO: geography? index?
);

CREATE INDEX zips_geom_spatial_idx ON zips_geom USING gist(geometry);

CREATE INDEX zips_geom_zip ON zips_geom (zipcode);

CREATE TABLE owner_taxpayer (
    owntax_id BIGINT PRIMARY KEY,
    taxpayer VARCHAR(256),
    own_id VARCHAR(256) -- TODO: indexing here?
);

CREATE TABLE taxpayer (
    tp_id BIGINT PRIMARY KEY,
    owntax_id BIGINT REFERENCES owner_taxpayer (owntax_id),
    taxpayer2 VARCHAR(256),
    tpaddr VARCHAR(256),
    tpcity VARCHAR(256),
    tpstate VARCHAR(256),
    tpzip VARCHAR(256),
    taxstatus VARCHAR(256)
);

CREATE TABLE taxpayer_property (
    taxparprop_id BIGINT PRIMARY KEY,
    tp_id BIGINT REFERENCES taxpayer (tp_id),
    prop_id BIGINT REFERENCES property (prop_id)
);

CREATE TABLE year (
    taxparprop_id BIGINT REFERENCES taxpayer_property (taxparprop_id),
    year INTEGER,
    saledate DATE,
    saleprice DOUBLE PRECISION,
    totsqft DOUBLE PRECISION,
    totacres DOUBLE PRECISION,
    cityrbuilt INT,
    resyrbuilt INT,
    PRIMARY KEY (taxparprop_id, year)
);

CREATE INDEX year_year_idx ON year (year);

CREATE MATERIALIZED VIEW owner_count AS (
    (
        SELECT
            DISTINCT ppg.year,
            STRING_AGG(DISTINCT ot.own_id, ',') AS own_id,
            COUNT(ot.own_id) AS count,
            CASE
                WHEN (
                    COUNT(ot.own_id) > 9
                    AND COUNT(ot.own_id) <= 20
                ) THEN 1
                WHEN (
                    COUNT(ot.own_id) > 20
                    AND COUNT(ot.own_id) <= 100
                ) THEN 2
                WHEN (
                    COUNT(ot.own_id) > 100
                    AND COUNT(ot.own_id) <= 200
                ) THEN 3
                WHEN (
                    COUNT(ot.own_id) > 200
                    AND COUNT(ot.own_id) <= 500
                ) THEN 4
                WHEN (
                    COUNT(ot.own_id) > 500
                    AND COUNT(ot.own_id) <= 1000
                ) THEN 5
                WHEN (
                    COUNT(ot.own_id) > 1000
                    AND COUNT(ot.own_id) <= 1500
                ) THEN 6
                WHEN (
                    COUNT(ot.own_id) > 1500
                ) THEN 7
            END AS own_group
        FROM
            parcel_property_geom AS ppg
            INNER JOIN property AS p ON ppg.prop_id = p.prop_id
            INNER JOIN taxpayer_property AS tp ON p.prop_id = tp.prop_id
            INNER JOIN taxpayer AS t ON tp.tp_id = t.tp_id
            INNER JOIN owner_taxpayer AS ot ON t.owntax_id = ot.owntax_id
        GROUP BY
            ppg.year,
            ot.own_id
    )
);

CREATE TABLE parcels (
    feature_id SERIAL,
    saledate DATE,
    saleprice DOUBLE PRECISION,
    totsqft DOUBLE PRECISION,
    totacres DOUBLE PRECISION,
    cityrbuilt INTEGER,
    resyrbuilt INTEGER,
    prop_id BIGINT,
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

CREATE INDEX parcels_year_idx ON parcels (year);

CREATE INDEX parcels_zipcode_idx ON parcels USING gin(propzip gin_trgm_ops);

CREATE INDEX parcels_spatial_idx ON parcels USING gist(centroid);

CREATE INDEX parcels_spatial_geom_idx ON parcels USING gist(geom);