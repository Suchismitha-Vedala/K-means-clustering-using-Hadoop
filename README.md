Hadoop Programming

To cluster amazon data, we are using K-means and canopy algorithms using MapReduce concept.

Enter the following Commands in the terminal

1. hdfs dfs -rm -r -f / intermediate /
2. ant clean
3. ant build
4. yarn jar build/lib/cloud_Suchi.jar src.{jar file of source code}  /{input-data} / intermediate /out
