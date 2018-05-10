#!/bin/bash

#spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python3 zip_code_shapely.py /user/tb1420/formatted_zip.txt /user/ms6771/crime_location.out/part-00000-53908aaa-bed4-4d05-b75d-9c13eb926925-c000.csv > spark_res.txt

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python3 zip_code_shapely.py /user/tb1420/formatted_zip.txt /user/ms6771/taxi_2012_locations.out/part-00000-f0006114-ae1e-4795-8431-cc6e3d6a52f6-c000.csv > spark_res_2.txt


