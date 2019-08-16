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
        String hdfsInput = "hdfs://hdfs-namenode:9000/schemas/";
        String stubPath = "hdfs://hdfs-namenode:9000/schemas/stub-meta/";
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parsing XML - Session")
                .master("local")
                .getOrCreate();
        
        Dataset<Row> pagelinks = spark.read()
                .format("avro")
                .load(hdfsInput + args[1])
                .filter("title = '" + args[0] + "'");
        
        Dataset<Row> revisions = spark.read()
                .format("avro")
                .load(stubPath + "stub-1.avsc"); //, stubPath + "stub-6.avsc"
        
        //pagelinks.filter("title = '" + args[0] + "'").collect();
        
        Dataset<Row> joined = pagelinks.join(revisions, "id");
        joined.show();
        
        joined.printSchema();
       
        //revisions.filter("title = '" + args[0] + "'").show();
        
        System.out.println( "Hello World!" );
    }
}
