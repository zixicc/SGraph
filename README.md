The basic version of Spark is 0.9.1, assume the directory of spark-0.9.1 is `$SPARK_HOME$`

Follow the step if you want to test SGraph:

1. Put the whole sgraph directory under `$SPARK_HOME$`
 
2. Replace `$SPARK_HOME$/project/SparkBuild.scala` with `configs/SparkBuild.scala`

3. Replace `$SPARK_HOME$/bin/compute-classpath.sh` with `configs/compute-classpath.sh`

4. Make `org.apache.spark.util.collection.PrimitiveVector` extends Serializable

5. Recompile Spark
