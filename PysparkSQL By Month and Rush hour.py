from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType
from csv import reader


spark = SparkSession.builder.appName("1004-proj").config("spark.driver.memory","700g").config("spark.executor.memory","700g").getOrCreate()
sc._conf.getAll()

#-----------------------------------------------------------------------------
##Load RAW Data##
#-----------------------------------------------------------------------------
collision = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/NYPD_Motor_Vehicle_Collisions.csv")
collision.createOrReplaceTempView("collision")

weather = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/weather-2011-2017.csv")
weather.createOrReplaceTempView("weather")

complaints = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/311/311_Service_Requests_from_20*.csv")
complaints.createOrReplaceTempView("complaints")

property = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/property_data/*.csv")
property.createOrReplaceTempView("property")

census_income_2011 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Household Income/ACS_11_5YR_B19001/ACS_11_5YR_B19001_with_ann.csv")
census_income_2011.createOrReplaceTempView("census_income_2011")

census_income_2012 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Household Income/ACS_12_5YR_B19001/ACS_12_5YR_B19001_with_ann.csv")
census_income_2012.createOrReplaceTempView("census_income_2012")

census_income_2013 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Household Income/ACS_13_5YR_B19001/ACS_13_5YR_B19001_with_ann.csv")
census_income_2013.createOrReplaceTempView("census_income_2013")

census_income_2014 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Household Income/ACS_14_5YR_B19001/ACS_14_5YR_B19001_with_ann.csv")
census_income_2014.createOrReplaceTempView("census_income_2014")

census_income_2015 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Household Income/ACS_15_5YR_B19001/ACS_15_5YR_B19001_with_ann.csv")
census_income_2015.createOrReplaceTempView("census_income_2015")

census_income_2016 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Household Income/ACS_16_5YR_B19001/ACS_16_5YR_B19001_with_ann.csv")
census_income_2016.createOrReplaceTempView("census_income_2016")

census_education_2011 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Education/ACS_11_5YR_S1501/ACS_11_5YR_S1501_with_ann.csv")
census_education_2011.createOrReplaceTempView("census_education_2011")

census_education_2012 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Education/ACS_12_5YR_S1501/ACS_12_5YR_S1501_with_ann.csv")
census_education_2012.createOrReplaceTempView("census_education_2012")

census_education_2013 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Education/ACS_13_5YR_S1501/ACS_13_5YR_S1501_with_ann.csv")
census_education_2013.createOrReplaceTempView("census_education_2013")

census_education_2014 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Education/ACS_14_5YR_S1501/ACS_14_5YR_S1501_with_ann.csv")
census_education_2014.createOrReplaceTempView("census_education_2014")

census_education_2015 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Education/ACS_15_5YR_S1501/ACS_15_5YR_S1501_with_ann.csv")
census_education_2015.createOrReplaceTempView("census_education_2015")

census_education_2016 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Census/Education/ACS_16_5YR_S1501/ACS_16_5YR_S1501_with_ann.csv")
census_education_2016.createOrReplaceTempView("census_education_2016")

crime = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/crime.csv")
crime.createOrReplaceTempView("crime")

citi_bike = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/citibike/*.csv")
citi_bike.createOrReplaceTempView("citi_bike")

taxi_2012 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Taxi-2011-2017/yellow_tripdata_2012*.csv")
taxi_2012.createOrReplaceTempView("taxi_2012")

taxi_2013 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Taxi-2011-2017/yellow_tripdata_2013*.csv")
taxi_2013.createOrReplaceTempView("taxi_2013")

taxi_2014 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Taxi-2011-2017/yellow_tripdata_2014*.csv")
taxi_2014.createOrReplaceTempView("taxi_2014")

taxi_2015 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Taxi-2011-2017/yellow_tripdata_2015*.csv")
taxi_2015.createOrReplaceTempView("taxi_2015")

taxi_2016 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/Taxi-2011-2017/yellow_tripdata_2016*.csv")
taxi_2016.createOrReplaceTempView("taxi_2016")

#-----------------------------------------------------------------------------
#Create Distinct Zipcode for Polygon Searching
#Load Polygon Searching Results
#-----------------------------------------------------------------------------
crime_location = spark.sql("\
select CMPLNT_NUM, \
Latitude as lat, \
Longitude as long \
from crime \
where CMPLNT_NUM is not null and Latitude is not null and Longitude is not null")
crime_location.createOrReplaceTempView("crime_location")

citi_bike_stations = spark.sql(" \
select distinct `start station id` as station_id, \
`start station latitude` as lat, \
`start station longitude` as long \
from citi_bike \
where `start station latitude` <> 'NULL' and `start station longitude` <> 'NULL' \
\
union \
\
select distinct `end station id` as station_id, \
`end station latitude` as lat, \
`end station longitude` as long \
from citi_bike \
where `end station latitude` <> 'NULL' and `end station longitude` <> 'NULL'")
citi_bike_stations.createOrReplaceTempView("citi_bike_stations")

taxi_2016_locations = spark.sql("\
select pickup_latitude, \
pickup_longitude \
from taxi_2016 \
where pickup_latitude is not null and pickup_longitude is not null \
and pickup_latitude not in ('null', 'NULL', 'Y', 'N') and pickup_longitude not in ('null', 'NULL', 'Y', 'N')\
group by pickup_latitude, \
pickup_longitude")
taxi_2016_locations.createOrReplaceTempView("taxi_2016_locations")

taxi_2015_locations = spark.sql("\
select pickup_latitude, \
pickup_longitude \
from taxi_2015 \
where pickup_latitude is not null and pickup_longitude is not null \
and pickup_latitude not in ('null', 'NULL', 'Y', 'N') and pickup_longitude not in ('null', 'NULL', 'Y', 'N')\
group by pickup_latitude, \
pickup_longitude")
taxi_2015_locations.createOrReplaceTempView("taxi_2015_locations")

taxi_2014_locations = spark.sql("\
select ` pickup_latitude` as pickup_latitude, \
` pickup_longitude` as pickup_longitude \
from taxi_2014 \
where ` pickup_latitude` is not null and ` pickup_longitude` is not null \
and ` pickup_latitude` not in ('null', 'NULL', 'Y', 'N') and ` pickup_longitude` not in ('null', 'NULL', 'Y', 'N')\
group by ` pickup_latitude`, \
` pickup_longitude`")
taxi_2014_locations.createOrReplaceTempView("taxi_2014_locations")

taxi_2013_locations = spark.sql("\
select pickup_latitude, \
pickup_longitude \
from taxi_2013 \
where pickup_latitude is not null and pickup_longitude is not null \
and pickup_latitude not in ('null', 'NULL', 'Y', 'N') and pickup_longitude not in ('null', 'NULL', 'Y', 'N')\
group by pickup_latitude, \
pickup_longitude")
taxi_2013_locations.createOrReplaceTempView("taxi_2013_locations")

taxi_2012_locations = spark.sql("\
select pickup_latitude, \
pickup_longitude \
from taxi_2012 \
where pickup_latitude is not null and pickup_longitude is not null \
and pickup_latitude not in ('null', 'NULL', 'Y', 'N') and pickup_longitude not in ('null', 'NULL', 'Y', 'N')\
group by pickup_latitude, \
pickup_longitude")
taxi_2012_locations.createOrReplaceTempView("taxi_2012_locations")

#Output-Long Lat
citi_bike_stations.coalesce(1).write.save("citi_bike_stations.out",format="csv",header="true")
crime_location.coalesce(1).write.save("crime_location.out",format="csv",header="true")
taxi_2016_locations.coalesce(1).write.save("taxi_2016_locations.out",format="csv",header="true")
taxi_2015_locations.coalesce(1).write.save("taxi_2015_locations.out",format="csv",header="true")
taxi_2014_locations.coalesce(1).write.save("taxi_2014_locations.out",format="csv",header="true")
taxi_2013_locations.coalesce(1).write.save("taxi_2013_locations.out",format="csv",header="true")
taxi_2012_locations.coalesce(1).write.save("taxi_2012_locations.out",format="csv",header="true")

crime_zip = sc.textFile("/user/tb1420/zip_crime.out1/part*")
crime_zip = crime_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["id", "zip_code"])
crime_zip.createOrReplaceTempView("crime_zip")

citi_bike_zip = sc.textFile("/user/tb1420/zip_citi.out/part-00000")
citi_bike_zip = citi_bike_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["id", "zip_code"])
citi_bike_zip.createOrReplaceTempView("citi_bike_zip")

taxi_2016_zip = sc.textFile("/user/tb1420/zip_taxi2016.out/part*")
taxi_2016_zip = taxi_2016_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["latitude", "longitude", "zip_code"])
taxi_2016_zip.createOrReplaceTempView("taxi_2016_zip")

taxi_2015_zip = sc.textFile("/user/tb1420/zip_taxi2015.out/part*")
taxi_2015_zip = taxi_2015_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["latitude", "longitude", "zip_code"])
taxi_2015_zip.createOrReplaceTempView("taxi_2015_zip")

taxi_2014_zip = sc.textFile("/user/tb1420/zip_taxi2014.out/part*")
taxi_2014_zip = taxi_2014_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["latitude", "longitude", "zip_code"])
taxi_2014_zip.createOrReplaceTempView("taxi_2014_zip")

taxi_2013_zip = sc.textFile("/user/tb1420/zip_taxi2013.out/part*")
taxi_2013_zip = taxi_2013_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["latitude", "longitude", "zip_code"])
taxi_2013_zip.createOrReplaceTempView("taxi_2013_zip")

taxi_2012_zip = sc.textFile("/user/tb1420/zip_taxi2012.out/part*")
taxi_2012_zip = taxi_2012_zip.mapPartitions(lambda x: reader(x, delimiter=' ')).toDF(["latitude", "longitude", "zip_code"])
taxi_2012_zip.createOrReplaceTempView("taxi_2012_zip")

#-----------------------------------------------------------------------------
#Create zipcode lookup table
#-----------------------------------------------------------------------------
borough_zip = spark.sql("\
select distinct borough, \
`ZIP CODE` as zip_code \
from collision")
borough_zip.createOrReplaceTempView("borough_zip")

#-----------------------------------------------------------------------------
#Data Cleaning and Aggregation by Month and Zip
#-----------------------------------------------------------------------------
weather_temp = spark.sql("\
select concat(year(TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp))),'/',month(TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp)))) as month,\
month(TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp))) as month_of_year, \
case when (time >= 700 and time <= 1000) or (time >= 1700 and time <= 2000) then 'R' else 'NR' end as rush_hour, \
case when `  Spd` like '999%' then null else `  Spd` end as wind_speed, \
case when cast(`  Visb` as float) like '999%' or cast(`  Visb` as float) = '' then null else cast(`  Visb` as float) end as visb, case when `  Temp`  like '999%' then null else `  Temp` end as temp, \
case when ` Prcp` like '999%' then null else ` Prcp` end as prcp, \
case when `  SD` like '999%' then null else `  SD` end as snow_depth, \
case when `    SDW` like '999%' then null else `    SDW` end as SDW, \
case when ` SA` like '999%' then null else ` SA` end as snow_accumulation \
from weather")
weather_temp.createOrReplaceTempView("weather_temp")
          
weather_cleaned = spark.sql("\
select month, \
month_of_year, \
rush_hour, \
avg(wind_speed) as wind_speed, \
avg(visb) as visb, \
avg(temp) as temp, \
avg(prcp) as prcp, \
avg(snow_depth) as snow_depth, \
avg(SDW) as SDW, \
avg(snow_accumulation) as snow_accumulation \
from weather_temp \
group by month, \
month_of_year, \
rush_hour")
weather_cleaned.createOrReplaceTempView("weather_cleaned")

collision_cleaned = spark.sql("\
SELECT concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))), '/', month(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)))) as month, \
month(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))) as month_of_year, \
case when (time >= '07:00' and time <= '10:00') or (time >= '17:00' and time <= '20:00') then 'R' else 'NR' end as rush_hour,\
rtrim(ltrim(`zip code`)) as zip_code, \
sum(cast(`NUMBER OF PERSONS INJURED` as int)) as number_persons_injured, \
sum(`NUMBER OF PERSONS KILLED`) as number_persons_killed, \
sum(`NUMBER OF PEDESTRIANS INJURED`) as number_pedestrians_injured, \
sum(`NUMBER OF PEDESTRIANS KILLED`) as number_pedestrians_killed, \
sum(`NUMBER OF CYCLIST INJURED`) as number_cyclist_injured, \
sum(cast(`NUMBER OF CYCLIST KILLED` as int)) as number_cyclist_killed, \
sum(cast(`NUMBER OF MOTORIST INJURED` as int)) as number_motorist_injured, \
sum(`NUMBER OF MOTORIST KILLED`) as number_motorist_killed \
FROM collision \
where rtrim(ltrim(`zip code`)) is not null and length(rtrim(ltrim(`zip code`))) = 5 \
GROUP BY concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))), '/', month(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)))), \
month(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))), \
case when (time >= '07:00' and time <= '10:00') or (time >= '17:00' and time <= '20:00') then 'R' else 'NR' end, \
rtrim(ltrim(`zip code`))")
collision_cleaned.createOrReplaceTempView("collision_cleaned")

complaints_cleaned = spark.sql("\
select concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))),'/',month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)))) as month, \
month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as month_of_year, \
case when (from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') >= 7 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 10) or (from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') >= 17 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 20) then 'R' else 'NR' end as rush_hour, \
`Complaint Type` as complaint_type, \
rtrim(ltrim(`Incident Zip`)) as zip_code, \
count(*) as complaint_count \
from complaints \
where rtrim(ltrim(`Incident Zip`)) is not null and length(rtrim(ltrim(`Incident Zip`))) = 5 \
group by concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))),'/',month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)))), \
month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))), \
case when (from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') >= 7 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 10) or (from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') >= 17 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 20) then 'R' else 'NR' end, \
`Complaint Type`, \
rtrim(ltrim(`Incident Zip`))")
complaints_cleaned.createOrReplaceTempView("complaints_cleaned")

property_cleaned = spark.sql("\
select `B` as borough, \
rtrim(ltrim(`ZIP`)) as zip_code, \
concat(substr(`YEAR`, 0, 2), substr(`YEAR`, 6, 8)) as year, \
`BLDGCL` as building_cl, \
`TAXCLASS` as tax_cl, \
AVG(`FULLVAL`) as avg_market_val, \
AVG(`AVLAND`) as avg_land_val, \
AVG(`AVTOT`) as avg_total_val, \
min(`FULLVAL`) as min_market_val, \
min(`AVLAND`) as min_land_val, \
min(`AVTOT`) as min_total_val, \
max(`FULLVAL`) as max_market_val, \
max(`AVLAND`) as max_land_val, \
max(`AVTOT`) as max_total_val \
from property \
where rtrim(ltrim(`ZIP`)) is not null and length(rtrim(ltrim(`ZIP`))) = 5 \
group by `ZIP`,\
`BLDGCL`,\
`TAXCLASS`,\
concat(substr(`YEAR`, 0, 2), substr(`YEAR`, 6, 8)),\
`B`")
property_cleaned.createOrReplaceTempView("property_cleaned")

census_education_cleaned = spark.sql("\
select '2011' as year, `GEO.id2` as zip_code, HC01_EST_VC16 as percent_high_school, HC01_EST_VC17 as percent_bachelor, HC01_EST_VC46 as median_earnings_high_school, HC01_EST_VC47 as median_earnings_bachelor from census_education_2011 where `GEO.id` <> 'Id' \
union all \
select '2012' as year, `GEO.id2` as zip_code, HC01_EST_VC16 as percent_high_school, HC01_EST_VC17 as percent_bachelor, HC01_EST_VC46 as median_earnings_high_school, HC01_EST_VC47 as median_earnings_bachelor from census_education_2012 where `GEO.id` <> 'Id' \
union all \
select '2013' as year, `GEO.id2` as zip_code, HC01_EST_VC16 as percent_high_school, HC01_EST_VC17 as percent_bachelor, HC01_EST_VC46 as median_earnings_high_school, HC01_EST_VC47 as median_earnings_bachelor from census_education_2013 where `GEO.id` <> 'Id' \
union all \
select '2014' as year, `GEO.id2` as zip_code, HC01_EST_VC16 as percent_high_school, HC01_EST_VC17 as percent_bachelor, HC01_EST_VC46 as median_earnings_high_school, HC01_EST_VC47 as median_earnings_bachelor from census_education_2014 where `GEO.id` <> 'Id' \
union all \
select '2015' as year, `GEO.id2` as zip_code, HC02_EST_VC17 as percent_high_school, HC02_EST_VC18 as percent_bachelor, HC01_EST_VC82 as median_earnings_high_school, HC01_EST_VC83 as median_earnings_bachelor from census_education_2015 where `GEO.id` <> 'Id' \
union all \
select '2016' as year, `GEO.id2` as zip_code, HC02_EST_VC17 as percent_high_school, HC02_EST_VC18 as percent_bachelor, HC01_EST_VC82 as median_earnings_high_school, HC01_EST_VC83 as median_earnings_bachelor from census_education_2016 where `GEO.id` <> 'Id'")
census_education_cleaned.createOrReplaceTempView("census_education_cleaned")

census_income_cleaned = spark.sql("\
select '2011' as year, \
`GEO.id2` as zip_code, \
HD01_VD01 as total_with_income, \
(HD01_VD02 + HD01_VD03 + HD01_VD04 + HD01_VD05 + HD01_VD06) / HD01_VD01 as 29999_less, \
(HD01_VD07 + HD01_VD08 + HD01_VD09 + HD01_VD10 + HD01_VD11 + HD01_VD12) / HD01_VD01 as 30000_to_74999, \
(HD01_VD13 + HD01_VD14 + HD01_VD15 + HD01_VD16 + HD01_VD17) / HD01_VD01 as 75000_more \
from census_income_2011 where `GEO.id` <> 'Id' \
\
union all \
\
select '2012' as year, \
`GEO.id2` as zip_code, \
HD01_VD01 as total_with_income, \
(HD01_VD02 + HD01_VD03 + HD01_VD04 + HD01_VD05 + HD01_VD06) / HD01_VD01 as 29999_less, \
(HD01_VD07 + HD01_VD08 + HD01_VD09 + HD01_VD10 + HD01_VD11 + HD01_VD12) / HD01_VD01 as 30000_to_74999, \
(HD01_VD13 + HD01_VD14 + HD01_VD15 + HD01_VD16 + HD01_VD17) / HD01_VD01 as 75000_more \
from census_income_2012 where `GEO.id` <> 'Id' \
\
union all \
\
select '2013' as year, \
`GEO.id2` as zip_code, \
HD01_VD01 as total_with_income, \
(HD01_VD02 + HD01_VD03 + HD01_VD04 + HD01_VD05 + HD01_VD06) / HD01_VD01 as 29999_less, \
(HD01_VD07 + HD01_VD08 + HD01_VD09 + HD01_VD10 + HD01_VD11 + HD01_VD12) / HD01_VD01 as 30000_to_74999, \
(HD01_VD13 + HD01_VD14 + HD01_VD15 + HD01_VD16 + HD01_VD17) / HD01_VD01 as 75000_more \
from census_income_2013 where `GEO.id` <> 'Id' \
\
union all \
\
select '2014' as year, \
`GEO.id2` as zip_code, \
HD01_VD01 as total_with_income, \
(HD01_VD02 + HD01_VD03 + HD01_VD04 + HD01_VD05 + HD01_VD06) / HD01_VD01 as 29999_less, \
(HD01_VD07 + HD01_VD08 + HD01_VD09 + HD01_VD10 + HD01_VD11 + HD01_VD12) / HD01_VD01 as 30000_to_74999, \
(HD01_VD13 + HD01_VD14 + HD01_VD15 + HD01_VD16 + HD01_VD17) / HD01_VD01 as 75000_more \
from census_income_2014 where `GEO.id` <> 'Id' \
\
union all \
\
select '2015' as year, \
`GEO.id2` as zip_code, \
HD01_VD01 as total_with_income, \
(HD01_VD02 + HD01_VD03 + HD01_VD04 + HD01_VD05 + HD01_VD06) / HD01_VD01 as 29999_less, \
(HD01_VD07 + HD01_VD08 + HD01_VD09 + HD01_VD10 + HD01_VD11 + HD01_VD12) / HD01_VD01 as 30000_to_74999, \
(HD01_VD13 + HD01_VD14 + HD01_VD15 + HD01_VD16 + HD01_VD17) / HD01_VD01 as 75000_more \
from census_income_2015 where `GEO.id` <> 'Id' \
\
union all \
\
select '2016' as year, \
`GEO.id2` as zip_code, \
HD01_VD01 as total_with_income, \
(HD01_VD02 + HD01_VD03 + HD01_VD04 + HD01_VD05 + HD01_VD06) / HD01_VD01 as 29999_less, \
(HD01_VD07 + HD01_VD08 + HD01_VD09 + HD01_VD10 + HD01_VD11 + HD01_VD12) / HD01_VD01 as 30000_to_74999, \
(HD01_VD13 + HD01_VD14 + HD01_VD15 + HD01_VD16 + HD01_VD17) / HD01_VD01 as 75000_more \
from census_income_2016 where `GEO.id` <> 'Id'")
census_income_cleaned.createOrReplaceTempView("census_income_cleaned")

crime_cleaned = spark.sql("\
select concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP))), '/', month(TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP)))) as month, \
month(TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP))) as month_of_year, \
case when (substr(CMPLNT_FR_TM, 0, 2) >= 7 and substr(CMPLNT_FR_TM, 0, 2) <= 10) or (substr(CMPLNT_FR_TM, 0, 2) >= 17 and substr(CMPLNT_FR_TM, 0, 2) <= 20) then 'R' else 'NR' end as rush_hour, \
B.zip_code, \
LAW_CAT_CD as crime_type, \
BORO_NM as borough, \
count(*) as count \
from crime A \
inner join crime_zip B on A.CMPLNT_NUM = B.id \
where BORO_NM is not null and TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP)) >= '2011-01-01'\
group by concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP))), '/', month(TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP)))), \
month(TO_DATE(CAST(UNIX_TIMESTAMP(CMPLNT_FR_DT, 'MM/dd/yyyy') AS TIMESTAMP))), \
case when (substr(CMPLNT_FR_TM, 0, 2) >= 7 and substr(CMPLNT_FR_TM, 0, 2) <= 10) or (substr(CMPLNT_FR_TM, 0, 2) >= 17 and substr(CMPLNT_FR_TM, 0, 2) <= 20) then 'R' else 'NR' end, \
B.zip_code, \
LAW_CAT_CD, \
BORO_NM")
crime_cleaned.createOrReplaceTempView("crime_cleaned")

citi_bike_cleaned = spark.sql("\
select case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'yyyy/MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'yyyy/MM') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'yyyy/MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') end) end as month, \
\
case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'MM') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') end) end as month_of_year, \
case when ((case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) >= 7 and (case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) <= 10) or ((case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) >= 17 and (case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) <= 20) then 'R' else 'NR' end as rush_hour, \
B.zip_code, \
avg(tripduration) as avg_trip_duration, \
sum(tripduration) as total_trip_duration, \
count(*) as total_trip_count \
from citi_bike A \
inner join citi_bike_zip B on A.`start station id` = B.id \
group by select case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'yyyy/MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'yyyy/MM') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'yyyy/MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') end) end, \
\
case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'MM') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') end) end , \
case when ((case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) >= 7 and (case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) <= 10) or ((case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) >= 17 and (case when \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) is null \
then \
from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm'), 'HH') \
else \
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'HH') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'HH') end) end) <= 20) then 'R' else 'NR' end, \
B.zip_code")
citi_bike_cleaned.createOrReplaceTempView("citi_bike_cleaned")

taxi_2016_cleaned = spark.sql(" \
select from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') as month, \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM') as month_of_year, \
case when (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 7 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 10) or (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 17 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 20) then 'R' else 'NR'  end as rush_hour, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2016 A \
inner join taxi_2016_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM'), \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM'), \
case when (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 7 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 10) or (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 17 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 20) then 'R' else 'NR' end, \
B.zip_code")
taxi_2016_cleaned.createOrReplaceTempView("taxi_2016_cleaned")

taxi_2015_cleaned = spark.sql(" \
select from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') as month, \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM') as month_of_year, \
case when (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 7 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 10) or (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 17 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 20) then 'R' else 'NR' end as rush_hour, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2015 A \
inner join taxi_2015_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM'), \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM'), \
case when (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 7 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 10) or (from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') >= 17 and from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'HH') <= 20) then 'R' else 'NR' end, \
B.zip_code")
taxi_2015_cleaned.createOrReplaceTempView("taxi_2015_cleaned")

taxi_2014_cleaned = spark.sql("\
select from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'yyyy/MM') as month, \
from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'MM') as month_of_year, \
case when (hour(` pickup_datetime`) >= 7 and hour(` pickup_datetime`) <= 10) or (hour(` pickup_datetime`) >= 17 and hour(` pickup_datetime`) <= 20) then 'R' else 'NR' end as rush_hour, \
B.zip_code, \
avg(` trip_distance`) as avg_trip_distance, \
sum(` trip_distance`) as total_trip_distance, \
avg(` total_amount`) as avg_amount, \
sum(` total_amount`) as total_amount \
from taxi_2014 A \
inner join taxi_2014_zip B on A.` pickup_latitude` = B.latitude and A.` pickup_longitude` = B.longitude \
group by from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'yyyy/MM'), \
from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'MM'), \
case when (hour(` pickup_datetime`) >= 7 and hour(` pickup_datetime`) <= 10) or (hour(` pickup_datetime`) >= 17 and hour(` pickup_datetime`) <= 20) then 'R' else 'NR' end, \
B.zip_code")
taxi_2014_cleaned.createOrReplaceTempView("taxi_2014_cleaned")

taxi_2013_cleaned = spark.sql(" \
select from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM') as month, \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM') as month_of_year, \
case when (hour(pickup_datetime) >= 7 and hour(pickup_datetime) <= 10) or (hour(pickup_datetime) >= 17 and hour(pickup_datetime) <= 20) then 'R' else 'NR' end as rush_hour, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2013 A \
inner join taxi_2013_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM'), \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM'), \
case when (hour(pickup_datetime) >= 7 and hour(pickup_datetime) <= 10) or (hour(pickup_datetime) >= 17 and hour(pickup_datetime) <= 20) then 'R' else 'NR' end, \
B.zip_code")
taxi_2013_cleaned.createOrReplaceTempView("taxi_2013_cleaned")

taxi_2012_cleaned = spark.sql("\
select from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM') as month, \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM') as month_of_year, \
case when (hour(pickup_datetime) >= 7 and hour(pickup_datetime) <= 10) or (hour(pickup_datetime) >= 17 and hour(pickup_datetime) <= 20) then 'R' else 'NR' end as rush_hour, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2012 A \
inner join taxi_2012_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM'), \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM'), \
case when (hour(pickup_datetime) >= 7 and hour(pickup_datetime) <= 10) or (hour(pickup_datetime) >= 17 and hour(pickup_datetime) <= 20) then 'R' else 'NR' end, \
B.zip_code")
taxi_2012_cleaned.createOrReplaceTempView("taxi_2012_cleaned")

taxi_cleaned = spark.sql("\
select * from taxi_2012_cleaned \
\
union all \
\
select * from taxi_2013_cleaned \
\
union all \
\
select * from taxi_2014_cleaned \
\
union all \
\
select * from taxi_2015_cleaned \
\
union all \
\
select * from taxi_2016_cleaned")
taxi_cleaned.createOrReplaceTempView("taxi_cleaned")


#-----------------------------------------------------------------------------
#Join
#Census Education & Census Income by year and zip
#-----------------------------------------------------------------------------
census_edu_income = spark.sql("\
select A.*, \
B.total_with_income, \
B.29999_less, \
B.30000_to_74999, \
B.75000_more \
from census_education_cleaned A \
inner join census_income_cleaned B on A.zip_code = B.zip_code and A.year = B.year")
census_edu_income.createOrReplaceTempView("census_edu_income")


#-----------------------------------------------------------------------------
#Output
#-----------------------------------------------------------------------------
weather_cleaned.coalesce(1).write.save("weather.out",format="csv",header="true")
collision_cleaned.coalesce(1).write.save("collision.out",format="csv",header="true")
complaints_cleaned.coalesce(1).write.save("complaintes.out",format="csv",header="true")
property_cleaned.coalesce(1).write.save("property.out",format="csv",header="true")
census_education_cleaned.coalesce(1).write.save("census_education.out",format="csv",header="true")
census_income_cleaned.coalesce(1).write.save("census_income.out",format="csv",header="true")

crime_cleaned.coalesce(1).write.save("crime.out",format="csv",header="true")
citi_bike_cleaned.coalesce(1).write.save("citibike.out",format="csv",header="true")
taxi_cleaned.coalesce(1).write.save("taxi.out",format="csv",header="true")

#Output-join
census_edu_income.coalesce(1).write.save("census_edu_income.out",format="csv",header="true")

#Output-borough zip
borough_zip.coalesce(1).write.save("borough_zip.out",format="csv",header="true")

