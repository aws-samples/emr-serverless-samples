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

CREATE EXTERNAL TABLE IF NOT EXISTS `noaa_gsod_pds`(
  `station` string, 
  `date` string, 
  `latitude` string, 
  `longitude` string, 
  `elevation` string, 
  `name` string, 
  `temp` string, 
  `temp_attributes` string, 
  `dewp` string, 
  `dewp_attributes` string, 
  `slp` string, 
  `slp_attributes` string, 
  `stp` string, 
  `stp_attributes` string, 
  `visib` string, 
  `visib_attributes` string, 
  `wdsp` string, 
  `wdsp_attributes` string, 
  `mxspd` string, 
  `gust` string, 
  `max` string, 
  `max_attributes` string, 
  `min` string, 
  `min_attributes` string, 
  `prcp` string, 
  `prcp_attributes` string, 
  `sndp` string, 
  `frshtt` string)
PARTITIONED BY (`year` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://noaa-gsod-pds/'
TBLPROPERTIES ('skip.header.line.count'='1');

ALTER TABLE noaa_gsod_pds ADD IF NOT EXISTS
  PARTITION (year='1929') LOCATION 's3://noaa-gsod-pds/1929/'
  PARTITION (year='1930') LOCATION 's3://noaa-gsod-pds/1930/'
  PARTITION (year='1931') LOCATION 's3://noaa-gsod-pds/1931/'
  PARTITION (year='1932') LOCATION 's3://noaa-gsod-pds/1932/'
  PARTITION (year='1933') LOCATION 's3://noaa-gsod-pds/1933/'
  PARTITION (year='1934') LOCATION 's3://noaa-gsod-pds/1934/'
  PARTITION (year='1935') LOCATION 's3://noaa-gsod-pds/1935/'
  PARTITION (year='1936') LOCATION 's3://noaa-gsod-pds/1936/'
  PARTITION (year='1937') LOCATION 's3://noaa-gsod-pds/1937/'
  PARTITION (year='1938') LOCATION 's3://noaa-gsod-pds/1938/'
  PARTITION (year='1939') LOCATION 's3://noaa-gsod-pds/1939/'
  PARTITION (year='1940') LOCATION 's3://noaa-gsod-pds/1940/'
  PARTITION (year='1941') LOCATION 's3://noaa-gsod-pds/1941/'
  PARTITION (year='1942') LOCATION 's3://noaa-gsod-pds/1942/'
  PARTITION (year='1943') LOCATION 's3://noaa-gsod-pds/1943/'
  PARTITION (year='1944') LOCATION 's3://noaa-gsod-pds/1944/'
  PARTITION (year='1945') LOCATION 's3://noaa-gsod-pds/1945/'
  PARTITION (year='1946') LOCATION 's3://noaa-gsod-pds/1946/'
  PARTITION (year='1947') LOCATION 's3://noaa-gsod-pds/1947/'
  PARTITION (year='1948') LOCATION 's3://noaa-gsod-pds/1948/'
  PARTITION (year='1949') LOCATION 's3://noaa-gsod-pds/1949/'
  PARTITION (year='1950') LOCATION 's3://noaa-gsod-pds/1950/'
  PARTITION (year='1951') LOCATION 's3://noaa-gsod-pds/1951/'
  PARTITION (year='1952') LOCATION 's3://noaa-gsod-pds/1952/'
  PARTITION (year='1953') LOCATION 's3://noaa-gsod-pds/1953/'
  PARTITION (year='1954') LOCATION 's3://noaa-gsod-pds/1954/'
  PARTITION (year='1955') LOCATION 's3://noaa-gsod-pds/1955/'
  PARTITION (year='1956') LOCATION 's3://noaa-gsod-pds/1956/'
  PARTITION (year='1957') LOCATION 's3://noaa-gsod-pds/1957/'
  PARTITION (year='1958') LOCATION 's3://noaa-gsod-pds/1958/'
  PARTITION (year='1959') LOCATION 's3://noaa-gsod-pds/1959/'
  PARTITION (year='1960') LOCATION 's3://noaa-gsod-pds/1960/'
  PARTITION (year='1961') LOCATION 's3://noaa-gsod-pds/1961/'
  PARTITION (year='1962') LOCATION 's3://noaa-gsod-pds/1962/'
  PARTITION (year='1963') LOCATION 's3://noaa-gsod-pds/1963/'
  PARTITION (year='1964') LOCATION 's3://noaa-gsod-pds/1964/'
  PARTITION (year='1965') LOCATION 's3://noaa-gsod-pds/1965/'
  PARTITION (year='1966') LOCATION 's3://noaa-gsod-pds/1966/'
  PARTITION (year='1967') LOCATION 's3://noaa-gsod-pds/1967/'
  PARTITION (year='1968') LOCATION 's3://noaa-gsod-pds/1968/'
  PARTITION (year='1969') LOCATION 's3://noaa-gsod-pds/1969/'
  PARTITION (year='1970') LOCATION 's3://noaa-gsod-pds/1970/'
  PARTITION (year='1971') LOCATION 's3://noaa-gsod-pds/1971/'
  PARTITION (year='1972') LOCATION 's3://noaa-gsod-pds/1972/'
  PARTITION (year='1973') LOCATION 's3://noaa-gsod-pds/1973/'
  PARTITION (year='1974') LOCATION 's3://noaa-gsod-pds/1974/'
  PARTITION (year='1975') LOCATION 's3://noaa-gsod-pds/1975/'
  PARTITION (year='1976') LOCATION 's3://noaa-gsod-pds/1976/'
  PARTITION (year='1977') LOCATION 's3://noaa-gsod-pds/1977/'
  PARTITION (year='1978') LOCATION 's3://noaa-gsod-pds/1978/'
  PARTITION (year='1979') LOCATION 's3://noaa-gsod-pds/1979/'
  PARTITION (year='1980') LOCATION 's3://noaa-gsod-pds/1980/'
  PARTITION (year='1981') LOCATION 's3://noaa-gsod-pds/1981/'
  PARTITION (year='1982') LOCATION 's3://noaa-gsod-pds/1982/'
  PARTITION (year='1983') LOCATION 's3://noaa-gsod-pds/1983/'
  PARTITION (year='1984') LOCATION 's3://noaa-gsod-pds/1984/'
  PARTITION (year='1985') LOCATION 's3://noaa-gsod-pds/1985/'
  PARTITION (year='1986') LOCATION 's3://noaa-gsod-pds/1986/'
  PARTITION (year='1987') LOCATION 's3://noaa-gsod-pds/1987/'
  PARTITION (year='1988') LOCATION 's3://noaa-gsod-pds/1988/'
  PARTITION (year='1989') LOCATION 's3://noaa-gsod-pds/1989/'
  PARTITION (year='1990') LOCATION 's3://noaa-gsod-pds/1990/'
  PARTITION (year='1991') LOCATION 's3://noaa-gsod-pds/1991/'
  PARTITION (year='1992') LOCATION 's3://noaa-gsod-pds/1992/'
  PARTITION (year='1993') LOCATION 's3://noaa-gsod-pds/1993/'
  PARTITION (year='1994') LOCATION 's3://noaa-gsod-pds/1994/'
  PARTITION (year='1995') LOCATION 's3://noaa-gsod-pds/1995/'
  PARTITION (year='1996') LOCATION 's3://noaa-gsod-pds/1996/'
  PARTITION (year='1997') LOCATION 's3://noaa-gsod-pds/1997/'
  PARTITION (year='1998') LOCATION 's3://noaa-gsod-pds/1998/'
  PARTITION (year='1999') LOCATION 's3://noaa-gsod-pds/1999/'
  PARTITION (year='2000') LOCATION 's3://noaa-gsod-pds/2000/'
  PARTITION (year='2001') LOCATION 's3://noaa-gsod-pds/2001/'
  PARTITION (year='2002') LOCATION 's3://noaa-gsod-pds/2002/'
  PARTITION (year='2003') LOCATION 's3://noaa-gsod-pds/2003/'
  PARTITION (year='2004') LOCATION 's3://noaa-gsod-pds/2004/'
  PARTITION (year='2005') LOCATION 's3://noaa-gsod-pds/2005/'
  PARTITION (year='2006') LOCATION 's3://noaa-gsod-pds/2006/'
  PARTITION (year='2007') LOCATION 's3://noaa-gsod-pds/2007/'
  PARTITION (year='2008') LOCATION 's3://noaa-gsod-pds/2008/'
  PARTITION (year='2009') LOCATION 's3://noaa-gsod-pds/2009/'
  PARTITION (year='2010') LOCATION 's3://noaa-gsod-pds/2010/'
  PARTITION (year='2011') LOCATION 's3://noaa-gsod-pds/2011/'
  PARTITION (year='2012') LOCATION 's3://noaa-gsod-pds/2012/'
  PARTITION (year='2013') LOCATION 's3://noaa-gsod-pds/2013/'
  PARTITION (year='2014') LOCATION 's3://noaa-gsod-pds/2014/'
  PARTITION (year='2015') LOCATION 's3://noaa-gsod-pds/2015/'
  PARTITION (year='2016') LOCATION 's3://noaa-gsod-pds/2016/'
  PARTITION (year='2017') LOCATION 's3://noaa-gsod-pds/2017/'
  PARTITION (year='2018') LOCATION 's3://noaa-gsod-pds/2018/'
  PARTITION (year='2019') LOCATION 's3://noaa-gsod-pds/2019/'
  PARTITION (year='2020') LOCATION 's3://noaa-gsod-pds/2020/'
  PARTITION (year='2021') LOCATION 's3://noaa-gsod-pds/2021/'
  PARTITION (year='2022') LOCATION 's3://noaa-gsod-pds/2022/'
  PARTITION (year='2023') LOCATION 's3://noaa-gsod-pds/2023/';
