WITH source_data AS (
    SELECT
        T1.PCDS,
        T1.OSEAST1M,
        T1.OSNRTH1M,
        T1.long,
        T1.lat,
        T1.ctry,
        T1.rgn,
        T1.PCON,
        T1.ITL,
        T1.OSLAUA,
        T1.OSWARD,
        T1.eer,
        T1.lsoa01,
        T1.lsoa11,
        T1.ru11ind,
        T1.dointr,
        T1.doterm,
        T2.CTRY12NM,
        T3.PCON24NM,
        T4.ITL125CD,
        T4.ITL125NM,
        T4.ITL225CD,
        T4.ITL225NM,
        T4.ITL325CD,
        T4.ITL325NM,
        T4.LAU125CD,
        T4.LAU125NM,
        T5.LAD23NM,
        T6.WD24NM,
        T7.EER10NM,
        T8.RU11NM
    FROM {{ source('test_lbg', 'onspd_full') }} AS T1
    LEFT JOIN {{ source('test_lbg', 'COUNTRY_CODE') }} AS T2 ON T1.ctry = T2.CTRY12CD
    LEFT JOIN {{ source('test_lbg', 'Westminster_Parliamentary') }} AS T3 ON T1.PCON = T3.PCON24CD
    LEFT JOIN {{ source('test_lbg', 'ITL125') }} AS T4 ON T1.ITL = T4.LAU125CD
    LEFT JOIN {{ source('test_lbg', 'LA_UA') }} AS T5 ON T5.LAD23CD = T1.OSLAUA
    LEFT JOIN {{ source('test_lbg', 'ward_name') }} AS T6 ON T6.WD24CD = T1.OSWARD
    LEFT JOIN {{ source('test_lbg', 'EER') }} AS T7 ON T1.eer = T7.EER10CD
    LEFT JOIN {{ source('test_lbg', 'rural_urban') }} AS T8 ON T1.ru11ind = T8.RU11IND
),
intermediate_derivations AS (
    SELECT
        *,
        CASE
            WHEN STRPOS(PCDS, ' ') = 3 THEN CONCAT(SUBSTR(PCDS, 1, 2), ' ', SUBSTR(PCDS, 6, 3))
            WHEN STRPOS(PCDS, ' ') = 0 THEN CONCAT(SUBSTR(PCDS, 1, 4), ' ', SUBSTR(PCDS, 6, 3))
            WHEN STRPOS(PCDS, ' ') = 4 THEN CONCAT(SUBSTR(PCDS, 1, 3), ' ', SUBSTR(PCDS, 6, 3))
            WHEN STRPOS(PCDS, ' ') = 5 THEN PCDS
            ELSE PCDS
        END AS POST_CODE
    FROM source_data
)
SELECT
    d.POST_CODE,
    SUBSTR(d.POST_CODE, STRPOS(d.POST_CODE, ' ') + 1, 3) AS INBND_POST_CODE,
    TRIM(SUBSTR(d.POST_CODE, 1, STRPOS(d.POST_CODE, ' '))) AS OUTBND_POST_CODE,
    TRIM(SUBSTR(d.POST_CODE, 1, STRPOS(d.POST_CODE, ' '))) AS POSTAL_DSTRCT,
    SUBSTR(d.POST_CODE, 1, STRPOS(d.POST_CODE, ' ') + 1) AS POSTAL_SECTOR,
    CAST(d.OSEAST1M AS INTEGER) AS X_COORDN,
    CAST(d.OSNRTH1M AS INTEGER) AS Y_COORDN,
    CAST(d.long AS NUMERIC) AS LONGTD,
    CAST(d.lat AS NUMERIC) AS LATTID,
    COALESCE(
        CASE
            WHEN d.CTRY = 'K02000001' THEN 'United Kingdom'
            WHEN d.CTRY = 'K03000001' THEN 'Great Britain'
            WHEN d.CTRY = 'K04000001' THEN 'England and Wales'
            ELSE d.CTRY12NM
        END, 'Other'
    ) AS COUNTRY,
    d.rgn AS RGN,
    CASE
        WHEN d.RGN = 'E12000001' THEN 'North East'
        WHEN d.RGN = 'E12000002' THEN 'North West'
        WHEN d.RGN = 'E12000003' THEN 'Yorkshire and The Humber'
        WHEN d.RGN = 'E12000004' THEN 'East Midlands'
        WHEN d.RGN = 'E12000005' THEN 'West Midlands'
        WHEN d.RGN = 'E12000006' THEN 'Eastern'
        WHEN d.RGN = 'E12000007' THEN 'London'
        WHEN d.RGN = 'E12000008' THEN 'South East'
        WHEN d.RGN = 'E12000009' THEN 'South West'
        WHEN d.RGN = 'W99999999' THEN 'Wales'
        WHEN d.RGN = 'S99999999' THEN 'Scotland'
        WHEN d.RGN = 'N99999999' THEN 'Northern Ireland'
        WHEN d.RGN = 'L99999999' THEN 'Channel Islands'
        WHEN d.RGN = 'M99999999' THEN 'Isle of Man'
        ELSE 'Other'
    END AS REGION,
    d.PCON AS PCON,
    d.PCON24NM AS WESTMINSTER,
    d.ITL125CD AS NUTS1CD,
    d.ITL125NM AS NUTS1,
    d.ITL225CD AS NUTS2CD,
    d.ITL225NM AS NUTS2,
    d.ITL325CD AS NUTS3CD,
    d.ITL325NM AS NUTS3,
    d.LAU125CD AS LAU125CD,
    d.LAU125NM AS LAU1,
    d.OSLAUA AS OSLAUA,
    d.LAD23NM AS LAUA_NM,
    d.OSWARD AS OSWARD,
    d.WD24NM AS WARD_NM,
    COALESCE(REPLACE(d.EER10NM, '(pseudo)', ''), 'Other') AS EU_REGION,
    d.lsoa01 AS LSOA_2001,
    d.lsoa11 AS LSOA_2011,
    CASE
        WHEN d.RU11NM LIKE '%Urban%' THEN 'U'
        WHEN d.RU11NM LIKE '%Rural%' THEN 'R'
        WHEN d.RU11NM LIKE '%Scotland%' AND d.RU11NM LIKE '%Town%' THEN 'U'
        ELSE NULL
    END AS RURAL_IN,
    CAST(d.dointr AS INTEGER) AS POST_CODE_START_DT,
    CAST(d.doterm AS INTEGER) AS POST_CODE_END_DT,
    CASE
        WHEN d.DOTERM IS NULL THEN 1
        ELSE 3
    END AS POST_CODE_STATUS
FROM intermediate_derivations AS d