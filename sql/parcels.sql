INSERT INTO
    parcels (
        feature_id,
        saledate,
        saleprice,
        totsqft,
        totacres,
        cityrbuilt,
        resyrbuilt,
        prop_id,
        year,
        propaddr,
        own_id,
        taxpayer,
        count,
        own_group,
        parcelno,
        propno,
        propdir,
        propzip,
        propzip2,
        centroid,
        geom
    )
SELECT
    DISTINCT ON(parcelno, year) feature_id,
    saledate,
    saleprice,
    totsqft,
    totacres,
    cityrbuilt,
    resyrbuilt,
    prop_id,
    year,
    propaddr,
    own_id,
    taxpayer,
    count,
    own_group,
    parcelno,
    propno,
    propdir,
    propzip,
    propzip2,
    centroid,
    geom
FROM
    (
        SELECT
            DISTINCT ROW_NUMBER() OVER (
                ORDER BY
                    1
            ) AS feature_id,
            y.saledate,
            y.saleprice,
            y.totsqft,
            y.totacres,
            y.cityrbuilt,
            y.resyrbuilt,
            p.prop_id,
            ppg.year,
            p.parcelno,
            p.propaddr,
            ot.own_id,
            ot.taxpayer,
            count.count,
            CASE
                WHEN (
                    count.count > 9
                    AND count.count <= 20
                ) THEN 1
                WHEN (
                    count.count > 20
                    AND count.count <= 100
                ) THEN 2
                WHEN (
                    count.count > 100
                    AND count.count <= 200
                ) THEN 3
                WHEN (
                    count.count > 200
                    AND count.count <= 500
                ) THEN 4
                WHEN (
                    count.count > 500
                    AND count.count <= 1000
                ) THEN 5
                WHEN (
                    count.count > 1000
                    AND count.count <= 1500
                ) THEN 6
                WHEN (count.count > 1500) THEN 7
            END AS own_group,
            p.propno,
            p.propdir,
            p.propstr,
            p.propzip AS propzip2,
            p.zipcode_sj AS propzip,
            ppg.centroid AS centroid,
            ppg.geom AS geom
        FROM
            parcel_property_geom AS ppg
            INNER JOIN property AS p ON p.prop_id = ppg.prop_id
            INNER JOIN taxpayer_property AS tp ON p.prop_id = tp.prop_id
            INNER JOIN year AS y on (
                tp.taxparprop_id = y.taxparprop_id
                AND ppg.year = y.year
            )
            INNER JOIN taxpayer AS t ON tp.tp_id = t.tp_id
            INNER JOIN owner_taxpayer AS ot ON t.owntax_id = ot.owntax_id
            INNER JOIN (
                SELECT
                    ppg.year,
                    STRING_AGG(DISTINCT ot.own_id, ',') AS own_id,
                    COUNT(DISTINCT p.prop_id)
                FROM
                    parcel_property_geom AS ppg
                    INNER JOIN property AS p ON p.prop_id = ppg.prop_id
                    INNER JOIN taxpayer_property AS tp ON p.prop_id = tp.prop_id
                    INNER JOIN year AS y on tp.taxparprop_id = y.taxparprop_id
                    INNER JOIN taxpayer AS t ON tp.tp_id = t.tp_id
                    INNER JOIN owner_taxpayer AS ot ON t.owntax_id = ot.owntax_id
                GROUP BY
                    ppg.year,
                    ot.own_id
            ) AS count ON ppg.year = count.year
            AND ot.own_id = count.own_id
        WHERE
            count.count > 9
    ) AS q;