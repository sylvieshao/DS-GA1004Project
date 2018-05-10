from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType
from csv import reader


spark = SparkSession.builder.appName("1004-proj").config("spark.driver.memory","512g").config("spark.executor.memory","512g").config("spark.mesos.executor.memoryOverhead","512g").config("spark.yarn.executor.memoryOverhead", "512g").config("spark.yarn.driver.memoryOverhead","512g").config("spark.executor.cores", "8").config("spark.driver.cores", "8").getOrCreate()
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
#Data Cleaning and Aggregation by Day and Zip
#-----------------------------------------------------------------------------
weather_temp = spark.sql("\
select concat(year(TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp))),'/',month(TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp)))) as month,\
month(TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp))) as month_of_year, \
TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp)) as date, \
case when `  Spd` like '999%' then null else `  Spd` end as wind_speed, \
case when cast(`  Visb` as float) like '999%' or cast(`  Visb` as float) = '' then null else cast(`  Visb` as float) end as visb, case when `  Temp`  like '999%' then null else `  Temp` end as temp, \
case when ` Prcp` like '999%' then null else ` Prcp` end as prcp, \
case when `  SD` like '999%' then null else `  SD` end as snow_depth, \
case when `    SDW` like '999%' then null else `    SDW` end as SDW, \
case when ` SA` like '999%' then null else ` SA` end as snow_accumulation \
from weather")
weather_temp.createOrReplaceTempView("weather_temp")
          
weather_day_zip = spark.sql("\
select month, \
month_of_year, \
date, \
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
date")
weather_day_zip.createOrReplaceTempView("weather_day_zip")

collision_day_zip = spark.sql("\
SELECT concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))), '/', month(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)))) as month, \
month(TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))) as month_of_year, \
TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)) as date,\
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
TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)), \
rtrim(ltrim(`zip code`))")
collision_day_zip.createOrReplaceTempView("collision_day_zip")

complaints_day_zip = spark.sql("\
select concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))),'/',month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)))) as month, \
month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as month_of_year, \
TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)) as date, \
rtrim(ltrim(`Incident Zip`)) as zip_code, \
count(*) as complaint_count \
from complaints \
where rtrim(ltrim(`Incident Zip`)) is not null and length(rtrim(ltrim(`Incident Zip`))) = 5 \
group by concat(year(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))),'/',month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)))), \
month(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))), \
TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)), \
rtrim(ltrim(`Incident Zip`))")
complaints_day_zip.createOrReplaceTempView("complaints_day_zip")

citi_bike_day_zip = spark.sql("\
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
\
case when \
(case when TO_DATE(CAST(unix_timestamp(starttime,'MM/dd/yyyy HH:mm:ss') AS TIMESTAMP)) is null then TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) else TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) end) is null \
then \
TO_DATE(CAST(unix_timestamp(starttime,'MM/dd/yyyy HH:mm') AS TIMESTAMP)) \
else \
(case when TO_DATE(CAST(unix_timestamp(starttime,'MM/dd/yyyy HH:mm:ss') AS TIMESTAMP)) is null then TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) else TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) end) end as date, \
\
B.zip_code, \
avg(tripduration) as avg_trip_duration, \
sum(tripduration) as total_trip_duration, \
count(*) as total_trip_count \
from citi_bike A \
inner join citi_bike_zip B on A.`start station id` = B.id \
group by case when \
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
(case when from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') is null then from_unixtime(UNIX_TIMESTAMP(starttime, 'MM/dd/yyyy HH:mm:ss'), 'MM') else from_unixtime(UNIX_TIMESTAMP(starttime, 'yyyy-MM-dd HH:mm:ss'), 'MM') end) end, \
\
case when \
(case when TO_DATE(CAST(unix_timestamp(starttime,'MM/dd/yyyy HH:mm:ss') AS TIMESTAMP)) is null then TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) else TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) end) is null \
then \
TO_DATE(CAST(unix_timestamp(starttime,'MM/dd/yyyy HH:mm') AS TIMESTAMP)) \
else \
(case when TO_DATE(CAST(unix_timestamp(starttime,'MM/dd/yyyy HH:mm:ss') AS TIMESTAMP)) is null then TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) else TO_DATE(CAST(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) end) end, \
\
B.zip_code")
citi_bike_day_zip.createOrReplaceTempView("citi_bike_day_zip")

taxi_2016_day_zip = spark.sql(" \
select from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') as month, \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM') as month_of_year, \
TO_DATE(CAST(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) as date, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2016 A \
inner join taxi_2016_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM'), \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM'), \
TO_DATE(CAST(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)), \
B.zip_code")
taxi_2016_day_zip.createOrReplaceTempView("taxi_2016_day_zip")

taxi_2015_day_zip = spark.sql(" \
select from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM') as month, \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM') as month_of_year, \
TO_DATE(CAST(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) as date, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2015 A \
inner join taxi_2015_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'yyyy/MM'), \
from_unixtime(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss'), 'MM'), \
TO_DATE(CAST(unix_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)), \
B.zip_code")
taxi_2015_day_zip.createOrReplaceTempView("taxi_2015_day_zip")

taxi_2014_day_zip = spark.sql("\
select from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'yyyy/MM') as month, \
from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'MM') as month_of_year, \
TO_DATE(CAST(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy') AS TIMESTAMP)) as date, \
B.zip_code, \
avg(` trip_distance`) as avg_trip_distance, \
sum(` trip_distance`) as total_trip_distance, \
avg(` total_amount`) as avg_amount, \
sum(` total_amount`) as total_amount \
from taxi_2014 A \
inner join taxi_2014_zip B on A.` pickup_latitude` = B.latitude and A.` pickup_longitude` = B.longitude \
group by from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'yyyy/MM'), \
from_unixtime(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy'), 'MM'), \
TO_DATE(CAST(UNIX_TIMESTAMP(` pickup_datetime`, 'MM/dd/yyyy') AS TIMESTAMP)), \
B.zip_code")
taxi_2014_day_zip.createOrReplaceTempView("taxi_2014_day_zip")

taxi_2013_day_zip = spark.sql(" \
select from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM') as month, \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM') as month_of_year, \
TO_DATE(CAST(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy') AS TIMESTAMP)) as date, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2013 A \
inner join taxi_2013_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM'), \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM'), \
TO_DATE(CAST(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy') AS TIMESTAMP)), \
B.zip_code")
taxi_2013_day_zip.createOrReplaceTempView("taxi_2013_day_zip")

taxi_2012_day_zip = spark.sql("\
select from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM') as month, \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM') as month_of_year, \
TO_DATE(CAST(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy') AS TIMESTAMP)) as date, \
B.zip_code, \
avg(trip_distance) as avg_trip_distance, \
sum(trip_distance) as total_trip_distance, \
avg(total_amount) as avg_amount, \
sum(total_amount) as total_amount \
from taxi_2012 A \
inner join taxi_2012_zip B on A.pickup_latitude = B.latitude and A.pickup_longitude = B.longitude \
group by from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'yyyy/MM'), \
from_unixtime(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy'), 'MM'), \
TO_DATE(CAST(UNIX_TIMESTAMP(pickup_datetime, 'MM/dd/yyyy') AS TIMESTAMP)), \
B.zip_code")
taxi_2012_day_zip.createOrReplaceTempView("taxi_2012_day_zip")

taxi_day_zip = spark.sql("\
select * from taxi_2012_day_zip \
\
union all \
\
select * from taxi_2013_day_zip \
\
union all \
\
select * from taxi_2014_day_zip \
\
union all \
\
select * from taxi_2015_day_zip \
\
union all \
\
select * from taxi_2016_day_zip")
taxi_day_zip.createOrReplaceTempView("taxi_day_zip")


#-----------------------------------------------------------------------------
#Join
#-----------------------------------------------------------------------------
#Collision, Weather and Complaints by Zip and Date
col_wea_311_day_zip = spark.sql(" \
select D.month, \
D.month_of_year, \
D.date, \
D.zip_code, \
C.wind_speed, \
C.visb, \
C.temp, \
C.prcp, \
C.snow_depth, \
C.SDW, \
C.snow_accumulation, \
D.number_persons_injured, \
D.number_persons_killed, \
D.number_pedestrians_injured, \
D.number_pedestrians_killed, \
D.number_cyclist_injured, \
D.number_cyclist_killed, \
D.number_motorist_injured, \
D.number_motorist_killed, \
E.complaint_count \
from weather_day_zip C \
inner join collision_day_zip D on C.date = D.date \
inner join complaints_day_zip E on D.date = E.date and D.zip_code = E.zip_code")
col_wea_311_day_zip.createOrReplaceTempView("col_wea_311_day_zip")


#Citibike & Weather by Zip and Date
citi_wea_day_zip = spark.sql("\
select B.month, \
B.month_of_year, \
B.date, \
B.zip_code, \
B.avg_trip_duration as citi_avg_trip_duration, \
B.total_trip_duration as citi_total_trip_duration, \
B.total_trip_count as total_trip_count, \
C.wind_speed, \
C.visb, \
C.temp, \
C.prcp, \
C.snow_depth, \
C.SDW, \
C.snow_accumulation \
from citi_bike_day_zip B \
inner join weather_day_zip C on B.date = C.date")
citi_wea_day_zip.createOrReplaceTempView("citi_wea_day_zip")

#Taxi & Weather by Zip and Date
taxi_wea_day_zip = spark.sql("\
select A.month, \
A.month_of_year, \
A.date, \
A.zip_code, \
A.avg_trip_distance as taxi_avg_trip_distance, \
A.total_trip_distance as taxi_total_trip_distance, \
A.avg_amount as taxi_avg_amount, \
A.total_amount as taxi_total_amount, \
C.wind_speed, \
C.visb, \
C.temp, \
C.prcp, \
C.snow_depth, \
C.SDW, \
C.snow_accumulation \
from taxi_day_zip A \
inner join weather_day_zip C on A.date = C.date")
taxi_wea_day_zip.createOrReplaceTempView("taxi_wea_day_zip")

#-----------------------------------------------------------------------------
#Output
#-----------------------------------------------------------------------------
col_wea_311_day_zip.coalesce(1).write.save("col_wea_311_day_zip.out",format="csv",header="true")
citi_wea_day_zip.coalesce(1).write.save("citi_wea_day_zip.out",format="csv",header="true")
taxi_wea_day_zip.write.save("taxi_wea_day_zip.out",format="csv",header="true")

