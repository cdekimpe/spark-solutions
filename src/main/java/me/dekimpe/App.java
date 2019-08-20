package me.dekimpe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
        
        String pagelinksFile = "hdfs://hdfs-namenode:9000/schemas/pagelinks.avsc";
        String stubPath = "hdfs://hdfs-namenode:9000/schemas/";
        
        String subject = args[0];
        String[] schemas = new String[6];
        for (int u = 0; u < 6; u++) {
            int i = u+1;
            schemas[u] = stubPath + "stub-" + i + ".avsc";
        }
        
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parsing XML - Session")
                .master("spark://192.168.10.14:7077")
                .config("spark.executor.memory", "4g")
                .getOrCreate();
        
        Dataset<Row> pagelinks = spark.read()
                .format("avro")
                .load(pagelinksFile)
                .filter("title = '" + subject + "'")
                .withColumnRenamed("id", "pl_id")
                .withColumnRenamed("title", "pl_title");
        
        Dataset<Row> revisions = spark.read()
                .format("avro")
                .load(schemas); //, stubPath + "stub-6.avsc"
        
        Dataset<Row> joined = pagelinks.join(revisions, pagelinks.col("pl_id").equalTo(revisions.col("id")), "outer").where("pl_title = '" + subject + "' or title = '" + subject + "'").cache();
        joined.printSchema();
        joined.show(100, false);
        /*Dataset<Row> exploded = joined.select(joined.col("pl_id"), explode(joined.col("revision"))).groupBy("col.contributor.username").agg(count("*").as("NumberOfRevisions"));
        Dataset<Row> result = exploded.orderBy(exploded.col("NumberOfRevisions").desc()).cache();
        
        
        result.show();
        result.write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save("hdfs://hdfs-namenode:9000/output/" + args[0] + ".csv");*/
        
    }
}
