from pyspark import SparkConf, SparkContext
from csv import reader
import sys
import numpy as np
from shapely.geometry import Polygon, Point
'''
current codes is used for taxi files

'''

#config
conf = SparkConf().setAppName("test")
sc = SparkContext(conf=conf)

lines1 = sc.textFile(sys.argv[1], 1)
lines1 = lines1.map(lambda x: x.split(' '))

lines1_x = lines1.map(lambda x: (x[2], float(x[0])))
lines1_y = lines1.map(lambda x: (x[2], float(x[1])))
lines1 = lines1.map(lambda x: (x[2],[(float(x[0]),float(x[1]))]))


# find max, min for x,y for each zip and overall
zipcode_sorted = lines1_x.reduceByKey(lambda x,y: max(x,y)).sortByKey()
zipcodes_list = zipcode_sorted.map(lambda x: x[0]).collect()
zip_x_min_list = zipcode_sorted.map(lambda x: x[1]).collect()
zip_x_max_list = lines1_x.reduceByKey(lambda x,y: min(x,y)).sortByKey().map(lambda x: x[1]).collect()
zip_y_max_list = lines1_y.reduceByKey(lambda x,y: max(x,y)).sortByKey().map(lambda x: x[1]).collect()
zip_y_min_list = lines1_y.reduceByKey(lambda x,y: min(x,y)).sortByKey().map(lambda x: x[1]).collect()
zip_x_min_broad = sc.broadcast(zip_x_min_list)
zip_x_max_broad = sc.broadcast(zip_x_max_list)
zip_y_min_broad = sc.broadcast(zip_y_min_list)
zip_y_max_broad = sc.broadcast(zip_y_max_list)
y_min = lines1_y.sortBy(lambda x:x[1]).first()[1]
y_max = lines1_y.sortBy(lambda x:x[1], 0).first()[1]
x_max = lines1_x.sortBy(lambda x:x[1]).first()[1]
x_min = lines1_x.sortBy(lambda x:x[1], 0).first()[1]

#build grid
grid_x_v = np.linspace(x_min, x_max+0.1, 101)
grid_y_v = np.linspace(y_min, y_max+0.1, 101)
grid_x = sc.parallelize(list(zip(grid_x_v[:-1], grid_x_v[1:])))
grid_y = sc.parallelize(list(zip(grid_y_v[:-1], grid_y_v[1:])))
grid = grid_x.cartesian(grid_y)

# return grid idx
def grid_check(gridx_min,gridx_max, gridy_min,gridy_max):
    zipcode_pos_list = []
    zip_x_min = zip_x_min_broad.value
    zip_y_min = zip_y_min_broad.value
    zip_x_max = zip_x_max_broad.value
    zip_y_max = zip_y_max_broad.value
    for i in range(len(zip_x_min)):
        if (max(abs(gridx_min), abs(zip_x_min[i])) < min(abs(gridx_max), abs(zip_x_max[i]))) and (max(gridy_min, zip_y_min[i]) < min(gridy_max, zip_y_max[i])):
            zipcode_pos_list.append(i)
    return zipcode_pos_list

# set grid global variable
grid_xmin_broad = sc.broadcast(grid.map(lambda x: x[0][0]).collect())
grid_xmax_broad = sc.broadcast(grid.map(lambda x: x[0][1]).collect())
grid_ymin_broad = sc.broadcast(grid.map(lambda x: x[1][0]).collect())
grid_ymax_broad = sc.broadcast(grid.map(lambda x: x[1][1]).collect())

# match grid with zipcodes
grid_z = grid.map(lambda x: grid_check(x[0][0], x[0][1], x[1][0], x[1][1]))
grid_z_broad = sc.broadcast(grid_z.collect())

# form all convex poly
zip_list = lines1.reduceByKey(lambda x,y: x+y)
zip_convex = zip_list.map(lambda x:(x[0], Polygon(x[1])))
zip_convex = zip_convex.sortByKey()
zip_convex_list = zip_convex.map(lambda x: x[1]).collect()

# set zip global variable
zip_convex_broad = sc.broadcast(zip_convex_list)

zipcodes_broad = sc.broadcast(zipcodes_list)
# match grid with given long and lat
def zip_code_look_up(x, y):
    grid_xmin = grid_xmin_broad.value
    grid_ymin = grid_ymin_broad.value
    grid_xmax = grid_xmax_broad.value
    grid_ymax = grid_ymax_broad.value
    zip_codes = grid_z_broad.value
    #x = float(x)
    #y = float(y)
    for i in range(len(grid_xmin)):
        if abs(x) >= abs(grid_xmin[i]) and abs(x) < abs(grid_xmax[i]) and y >= grid_ymin[i] and y < grid_ymax[i]:
            return zip_codes[i]
    return -1

#read file needs to get zip
lines2 = sc.textFile(sys.argv[2], 1)
header = lines2.first()
lines2 = lines2.filter(lambda x: x != header)
lines2 = lines2.mapPartitions(lambda x: reader(x))

# match long, lat with grid and filter nonvalid (i.e. non nyc zip)
# in the following blocks, commented codes are used for non taxi files
#grid_idx = lines2.map(lambda x: (x[0],(x[2],x[1]),zip_code_look_up(float(x[2]),float(x[1]))))
grid_idx = lines2.map(lambda x: ((x[0],x[1]),zip_code_look_up(float(x[1]),float(x[0]))))
#grid_idx_filter = grid_idx.filter(lambda x: (x[2] != -1 and len(x[2]) != 0))
grid_idx_filter = grid_idx.filter(lambda x: (x[1] != -1 and len(x[1]) != 0))

# find zip
def find_zip(pt, zip_list):
    convex_p = zip_convex_broad.value
    zipc = zipcodes_broad.value
    if (len(zip_list)==1):
        return zipc[zip_list[0]]
    
    for i in zip_list:
        if convex_p[i].contains(pt):
            return zipc[i]
    return "NA"

# get result
# in the following blocks, commented codes are used for non taxi files                                                                            
#zip_try = grid_idx_filter.map(lambda x: (x[0], find_zip(Point(float(x[1][0]), float(x[1][1])),x[2])))
zip_try = grid_idx_filter.map(lambda x: (x[0], find_zip(Point(float(x[0][1]), float(x[0][0])),x[1]))) 
#zip_try.map(lambda x: x[0]+' '+x[1]).saveAsTextFile("zip_taxi2016.out")
zip_try.map(lambda x: x[0][0]+' '+x[0][1]+' '+x[1]).saveAsTextFile("zip_taxi2015.out")
