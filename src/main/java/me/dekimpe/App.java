package me.dekimpe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String stubPath = "hdfs://hdfs-namenode:9000/schemas/stub-meta/";
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parsing XML - Session")
                .master("local")
                .getOrCreate();
        
        Dataset<Row> df = spark.read()
                .format("avro")
                .option("basePath", "hdfs://hdfs-namenode:9000/schemas/stub-meta/")
                .load(stubPath + args[1], stubPath + args[2]);
        
        df.printSchema();
        
        df.select("title = '" + args[0] + "'").show();
        
        System.out.println( "Hello World!" );
    }
}
