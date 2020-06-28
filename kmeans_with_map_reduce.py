from math import radians, cos, sin, asin, sqrt, atan2
import operator
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("spark://master:7077", "kmeans")
spark = SparkSession.builder.appName('kmeans').getOrCreate()

def get_k_first_points(points, k):
	result = [(i, j) for i,j in points.head(k)]
	return result #return first k points from whole point dataframe

def haver_dist(point1, point2):
	lon1, lat1 = point1[0], point1[1]
	lon2, lat2 = point2[0], point2[1]
	#convert from decimal degrees to radians
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
  # haversine formula 
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * atan2(sqrt(a), sqrt(1-a)) 
	r = 6371 # Radius of earth in kilometers. Use 3956 for miles
	return c * r

def get_labels_for_each_point(point, centroids):
	distances = [(i, haver_dist(point, i)) for i in centroids] #collect them
	global centroids_dict
	min_dist_centroid = (min(distances, key = operator.itemgetter(1)))[0]
	label = centroids_dict[min_dist_centroid]
	return label


def writer_centr(centroids, iteration):
	res = 'Results/Centers for iteration {0}:\n'.format(iteration)
	for i in centroids:
		res += '\t(x, y) -> ({0}, {1})\n'.format(i[0], i[1])
	return res
	
#df = sc.textFile('/yellow_tripdata_1m.csv').cache()
#df1 = df.map(lambda line: line.split(','))
#df2 = df1.filter(lambda line: float(line[3])*float(line[4])!=0)
#points = df2.map(lambda x: (float(x[3]), float(x[4])))
#points = sc.textFile('/yellow_tripdata_1m.csv').map(lambda line: tuple(map(operator.mul, (float(line.split(',')[3]), float(line.split(',')[4])), tuple(2 * [float(line.split(',')[3])*float(line.split(',')[4]) != 0]) )))

points = sc.textFile('/yellow_tripdata_1m.csv').map(lambda line: ((float(line.split(',')[3]), float(line.split(',')[4])))).filter(lambda x: x[0] * x[1] != 0)
points_df = points.toDF()


k = 5
max_iterations = 3

result_file = 'Results for each iteration in form (x, y) = (lon, lat)\n\n'

centroids = get_k_first_points(points_df, k)

result_file += writer_centr(centroids, 0)

centroids_dict = {v: k for k, v in dict(enumerate(centroids)).items()} # dictionary of all centroids with its distinct label/index
iterations = 1

while(iterations <= max_iterations):
	iterations += 1
	z = lambda x,y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
	#label_points = points.map(lambda x: (get_labels_for_each_point(x, centroids), tuple(x + (1,)))).reduceByKey(z) #(label, [point, 1])
	
	adding_of_all = points.map(lambda x: (get_labels_for_each_point(x, centroids), tuple(x + (1,)))).reduceByKey(z) #(label, [point, 1])
	
	#z = lambda x,y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
	#adding_of_all = label_points.reduceByKey(z)
	
	new_centroids = adding_of_all.map(lambda x: (x[1][0] / x[1][2], x[1][1] / x[1][2])).collect()
		
	centroids = new_centroids
	
	result_file += writer_centr(centroids, iterations - 1)
	centroids_dict = {v: k for k, v in dict(enumerate(centroids)).items()} # update dict

#result_file += writer_centr(centroids, iterations - 1)

out = open('/home/user/results', 'a')
out.write(result_file)
out.close()

import subprocess as s
s.check_output('hadoop fs -put /home/user/results /final_results', shell=True)
out = s.check_output('hadoop fs -cat /final_results', shell=True)
print(str(out, 'utf-8'))


