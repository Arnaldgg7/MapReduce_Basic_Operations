# MapReduce_Basic_Operations
A set of several basic SQL operations performed by means of the MapReduce framework in a cluster environment with HDFS as a distributed file system and implemented in an efficient way. There are provided the 'Main' classes either for executing locally and test the code, or executed in a cluster environment.

The data set where these operations have been performed is a random dataset in a Sequence File format made with a JAR program that contains types of wines and different measures with regard to the specific attributes that can be measured in a wine.

The operations performed through MapReduce and its outputs are:
- Aggregation of a specific attributes from each wine by computing its mean per type of wine (aggregationAvg.out).
- Aggregation of a specific attributes from each wine by computing its sum per type of wine (aggregationSum.out).
- Cartesian product of all types of existing wines in the file (output not provided because of its size).
- Projection of some specific wine attributes (projection.out).
- Selection of some specific types of wines in the overall data set (selection.out).
