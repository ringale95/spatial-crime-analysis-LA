
CREATE DATABASE IF NOT EXISTS crime_analysis
LOCATION '/hive/warehouse/crime_analysis.db';

USE crime_analysis;

DROP TABLE IF EXISTS crime_analysis.crimes_with_location;

CREATE EXTERNAL TABLE IF NOT EXISTS crime_analysis.crimes_with_location (
     dr_no STRING,
  date_rptd STRING,
  date_occ STRING,
  time_occ STRING,
  area STRING,
  rpt_dist_no STRING,
  part_1_2 INT,
  crm_cd STRING,
  mocodes STRING,
  vict_age INT,
  vict_sex STRING,
  vict_descent STRING,
  premis_cd STRING,
  weapon_used_cd STRING,
  status STRING,
  location STRING,
  cross_street STRING,
  lat DOUBLE,
  lon DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/crime_data_analysis/output/final_crimes_data/'
TBLPROPERTIES ('skip.header.line.count'='1');


SELECT * FROM crimes_with_location LIMIT 10;
