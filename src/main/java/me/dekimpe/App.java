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
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parsing XML - Session")
                .master("local")
                .getOrCreate();
        
        Dataset<Row> df = spark.read()
                .format("avro")
                .load("hdfs://hdfs-namenode:9000/schemas/" + args[0]);
        
        df.printSchema();
        
        System.out.println( "Hello World!" );
    }
}
