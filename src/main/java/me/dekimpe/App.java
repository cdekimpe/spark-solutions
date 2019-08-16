package me.dekimpe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;

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
        
        Dataset<Row> joined = pagelinks.join(revisions, "id").cache();
        Dataset<Row> exploded = joined.select(joined.col("id"), joined.col("title"), explode(joined.col("revision")));
        Dataset<Row> result = exploded.groupBy("contributor.name").agg(count("*").as("Number of revisions"));
        
        System.out.println(result.orderBy("Number of revisions").take(5));
        System.out.println("Number of rows : " + joined.count());
        System.out.println( "Hello World!" );
    }
}
