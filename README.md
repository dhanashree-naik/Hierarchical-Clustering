# Hierarchical-Clustering
### data:
 https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
 
 Problem statement:
1. Read the data from the file. Use only the floating-point values for the clustering. Don’t discard the class
information.
2. Apply the hierarchical algorithm to find clusters. Use Euclidean distance as your distance measure.
3. Assign each final cluster a name by choosing the most frequently occurring class label of the examples in the
cluster.
4. Count the number of data points that were put in each cluster.
5. Find the number of data points that were put in clusters in which they didn’t belong (based on having a
different class label than the cluster name)

Algorithm:
you will implement Hierarchical Clustering, which should start from merging the first two closest points, then the next closest, etc. Euclidean distance is used as the distance metric, and the coordinate of centroid is defined as the average of that of all the points in the cluster.
Priority Queue can be used to build the full hierarchy. To initial a priority queue, you can use the heapq library in Python and scala.collection.mutable.PriorityQueue library in Scala.
