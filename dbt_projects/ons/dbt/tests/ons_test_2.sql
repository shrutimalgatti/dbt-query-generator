output_file_name: assert_onsfdp_region_derivation.sql
select
    rgn,
    region
from {{ ref('onsfdp') }}
where
    region <>
        case
            when rgn = 'E12000001' then 'North East'
            when rgn = 'E12000002' then 'North West'
            when rgn = 'E12000003' then 'Yorkshire and The Humber'
            when rgn = 'E12000004' then 'East Midlands'
            when rgn = 'E12000005' then 'West Midlands'
            when rgn = 'E12000006' then 'Eastern'
            when rgn = 'E12000007' then 'London'
            when rgn = 'E12000008' then 'South East'
            when rgn = 'E12000009' then 'South West'
            when rgn = 'W99999999' then 'Wales'
            when rgn = 'S99999999' then 'Scotland'
            when rgn = 'N99999999' then 'Northern Ireland'
            when rgn = 'L99999999' then 'Channel Islands'
            when rgn = 'M99999999' then 'Isle of Man'
            else 'Other'
        end