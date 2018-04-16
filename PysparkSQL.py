from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("1004-proj").config("some-config","some-val").getOrCreate()

collision = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/NYPD_Motor_Vehicle_Collisions.csv")

collision.createOrReplaceTempView("collision")

weather = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/weather-2011-2017.csv")

weather.createOrReplaceTempView("weather")

complaints = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ms6771/project/311_Service_Requests_from_20*.csv")

complaints.createOrReplaceTempView("complaints")

collision_cleaned = spark.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)) as date, case when time > '06:00' and time <= '10:00' then 'A' when time > '10:00' and time <= '12:00' then 'B' when time > '12:00' and time <= '14:00' then 'C' when time > '14:00' and time <= '16:00' then 'D' when time > '16:00' and time <= '19:00' then 'E' when time > '19:00' and time <= '22:00' then 'F' else 'G' end as time_slot, rtrim(ltrim(`zip code`)) as zip_code, sum(cast(`NUMBER OF PERSONS INJURED` as int)) as number_persons_injured, sum(`NUMBER OF PERSONS KILLED`) as number_persons_killed, sum(`NUMBER OF PEDESTRIANS INJURED`) as number_pedestrians_injured, sum(`NUMBER OF PEDESTRIANS KILLED`) as number_pedestrians_killed, sum(`NUMBER OF CYCLIST INJURED`) as number_cyclist_injured, sum(cast(`NUMBER OF CYCLIST KILLED` as int)) as number_cyclist_killed, sum(cast(`NUMBER OF MOTORIST INJURED` as int)) as number_motorist_injured, sum(`NUMBER OF MOTORIST KILLED`) as number_motorist_killed FROM collision where rtrim(ltrim(`zip code`)) is not null GROUP BY TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP)), case when time > '06:00' and time <= '10:00' then 'A' when time > '10:00' and time <= '12:00' then 'B' when time > '12:00' and time <= '14:00' then 'C' when time > '14:00' and time <= '16:00' then 'D' when time > '16:00' and time <= '19:00' then 'E' when time > '19:00' and time <= '22:00' then 'F' else 'G' end, rtrim(ltrim(`zip code`))")

collision_cleaned.createOrReplaceTempView("collision_cleaned")

weather_temp = spark.sql("select TO_DATE(cast(UNIX_TIMESTAMP(cast(`    Date` as string), 'yyyyMMdd') as timestamp)) as date, case when time > 600 and time <= 1000 then 'A' when time > 1000 and time <= 1200 then 'B' when time > 1200 and time <= 1400 then 'C' when time > 1400 and time <= 1600 then 'D' when time > 1600 and time <= 1900 then 'E' when time > 1900 and time <= 2200 then 'F' else 'G' end as time_slot, case when `  Spd` like '999%' then null else `  Spd` end as wind_speed, case when cast(`  Visb` as float) like '999%' or cast(`  Visb` as float) = '' then null else cast(`  Visb` as float) end as visb, case when `  Temp`  like '999%' then null else `  Temp` end as temp, case when ` Prcp` like '999%' then null else ` Prcp` end as prcp, case when `  SD` like '999%' then null else `  SD` end as snow_depth, case when `    SDW` like '999%' then null else `    SDW` end as SDW, case when ` SA` like '999%' then null else ` SA` end as snow_accumulation from weather")

weather_temp.createOrReplaceTempView("weather_temp")

weather_cleaned = spark.sql("select date, time_slot, avg(wind_speed) as wind_speed, avg(visb) as visb, avg(temp) as temp, avg(prcp) as prcp, avg(snow_depth) as snow_depth, avg(SDW) as SDW, avg(snow_accumulation) as snow_accumulation from weather_temp group by date, time_slot")

weather_cleaned.createOrReplaceTempView("weather_cleaned")

complaints_cleaned = spark.sql("select TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)) as created_date, case when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 6 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 10 then 'A' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 10 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 12 then 'B' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 12 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 14 then 'C' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 14 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 16 then 'D' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 16 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 19 then 'E' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 19 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 22 then 'F' else 'G' end as time_slot, `Complaint Type` as complaint_type, rtrim(ltrim(`Incident Zip`)) as incident_zip, count(*) as complaint_count from complaints group by TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)), case when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 6 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 10 then 'A' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 10 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 12 then 'B' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 12 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 14 then 'C' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 14 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 16 then 'D' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 16 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 19 then 'E' when from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') > 19 and from_unixtime(unix_timestamp(`Created Date`,'MM/dd/yyyy hh:mm:ss aa'),'HH') <= 22 then 'F' else 'G' end, `Complaint Type`, rtrim(ltrim(`Incident Zip`))")

complaints_cleaned.createOrReplaceTempView("complaints_cleaned")

#Collision, complaints, weather join
spark.sql("select A.*, B.complaint_count, C.wind_speed, C.visb, C.temp, C.prcp, C.snow_depth, C.SDW, C.snow_accumulation from collision_cleaned A left outer join (select created_date, time_slot, incident_zip, sum(complaint_count) as complaint_count from complaints_cleaned group by created_date, time_slot, incident_zip) B on A.date = B.created_date and A.time_slot = B.time_slot and A.zip_code = B.incident_zip left outer join weather_cleaned C on A.date = C.date and A.time_slot = C.time_slot where A.date is not null and B.created_date is not null and A.zip_code is not null").coalesce(1).write.save("Collision_Complaints_Weather.out",format="csv",header="true")

#Collision join Weather
#spark.sql("select A.*, B.wind_speed, B.visb, B.temp, B.prcp, B.snow_depth, B.SDW, B.snow_accumulation from collision_cleaned A left outer join weather_cleaned B on A.date = B.date and A.time_slot = B.time_slot").coalesce(1).write.save("Collision_Weather.out",format="csv",header="true")

#Complaint types
#spark.sql("select distinct complaint_type from complaints_cleaned order by complaint_type").coalesce(1).write.save("complaint_type.out", format="text")


