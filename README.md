## Advanced Databases NTUA
Map Reduce approach of the k-means algorithm.  
Data taken from HDFS file.  
Data contain trip records from all trips completed in yellow taxis in NYC from 1/2015 to 6/2015.  
Algorithm returns top five central points' coordinates.

## How to run  
1. Install pyspark  
```
pip3 install pyspark
```

2. Upload data in Hadoop Distributed File System (HDFS)  
```
hadoop fs -put ./yellow_tripdata_1m.csv hdfs://master:9000/yellow_tripdata_1m.csv
```

3. Submit task in Spark environment   
```
spark-submit kmeans_with_map_reduce.py
```

4. Get Results to Local File  
```
hadoop fs -getmerge hdfs://master:9000/kmeans_with_map_reduce.results ./kmeans_with_map_reduce.results
```

5. Access Results  
```
cat map_reduce_kmeans.res 
```
