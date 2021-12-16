-- MIT No Attribution

-- Copyright 2021 Amazon.com, Inc. or its affiliates

-- Permission is hereby granted, free of charge, to any person obtaining a copy of this
-- software and associated documentation files (the "Software"), to deal in the Software
-- without restriction, including without limitation the rights to use, copy, modify,
-- merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
-- permit persons to whom the Software is furnished to do so.

-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
-- INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
-- PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
-- HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
-- OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
-- SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

SET hive.cli.print.header=true;
SET hive.query.name=ExtremeWeather;

WITH noaa_year_data AS (
    SELECT
        station,
        name,
        `date`,
        latitude,
        longitude,
        CAST(elevation AS double) AS elevation,
        CAST(temp AS double) AS avg_daily_temp,
        CASE WHEN CAST(max AS double) = 9999.9 THEN NULL ELSE CAST(max AS double) END AS max_temp,
        CASE WHEN CAST(wdsp AS double) = 999.9 THEN NULL ELSE CAST(wdsp AS double) END AS wind_speed,
        CASE WHEN CAST(gust AS double) = 999.9 THEN NULL ELSE CAST(gust AS double) END AS wind_gust_speed,
        CASE WHEN CAST(prcp AS double) = 99.9 THEN NULL ELSE CAST(prcp AS double) END AS precipitation
    FROM noaa_gsod_pds
    WHERE year = '2021'
),

rankings AS (
    SELECT *,
        rank() OVER (ORDER BY max_temp DESC) AS max_tmp_rnk,
        rank() OVER (ORDER BY avg_daily_temp DESC) AS max_avg_daily_temp_rnk,
        rank() OVER (ORDER BY wind_speed DESC) AS max_windspeed_rnk
    FROM noaa_year_data
),

rankings_desc AS (
    SELECT 
        CASE WHEN max_tmp_rnk=1 THEN 'max_temp'
            WHEN max_avg_daily_temp_rnk=1 THEN 'max_avg_daily_temp'
            WHEN max_windspeed_rnk=1 THEN 'max_wind_speed'
            ELSE NULL
        END AS max_attribute,
        CASE WHEN max_tmp_rnk=1 THEN max_temp
            WHEN max_avg_daily_temp_rnk=1 THEN avg_daily_temp
            WHEN max_windspeed_rnk=1 THEN wind_speed
            ELSE NULL
        END AS max_value,
        *
    FROM rankings
    WHERE max_tmp_rnk=1 OR max_avg_daily_temp_rnk=1 OR max_windspeed_rnk=1
)

SELECT
    station,
    name,
    `date`,
    max_attribute,
    max_value,
    latitude,
    longitude,
    avg_daily_temp,
    max_temp,
    wind_speed,
    wind_gust_speed,
    precipitation
FROM rankings_desc;
