package tn.enit.bigdata.processor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;

import tn.enit.bigdata.entity.AverageData;
import tn.enit.bigdata.entity.CarLocation;
import org.apache.spark.sql.expressions.Window;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class ProcessorUtils {

    public static SparkConf getSparkConf(Properties prop) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(prop.getProperty("tn.enit.bigdata.spark.app.name"))
                .setMaster(prop.getProperty("tn.enit.bigdata.spark.master"))
                .set("spark.cassandra.connection.host", prop.getProperty("tn.enit.bigdata.cassandra.host"))
                .set("spark.cassandra.connection.port", prop.getProperty("tn.enit.bigdata.cassandra.port"))
                .set("spark.cassandra.auth.username", prop.getProperty("tn.enit.bigdata.cassandra.username"))
                .set("spark.cassandra.auth.password", prop.getProperty("tn.enit.bigdata.cassandra.password"))
                .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("tn.enit.bigdata.cassandra.keep_alive"));


 
        if ("local".equals(prop.getProperty("tn.enit.bigdata.env"))) {
            sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
        }
        return sparkConf;
    }

 
    public static void saveLocationToCassandra(final JavaDStream<CarLocation> dataStream) {
        System.out.println("Saving to Cassandra...");

    
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("carid", "carid");
        columnNameMappings.put("timestamp", "timestamp");
        columnNameMappings.put("latitude", "latitude");
        columnNameMappings.put("longitude", "longitude");


     
        javaFunctions(dataStream).writerBuilder("carlocationkeyspace", "car_location",
                CassandraJavaUtil.mapToRow(CarLocation.class, columnNameMappings)).saveToCassandra();
             
                javaFunctions(dataStream).writerBuilder("carlocationkeyspace", "live",
                CassandraJavaUtil.mapToRow(CarLocation.class, columnNameMappings)).saveToCassandra();
    }

    public static void saveAvgToCassandra(JavaRDD<AverageData> rdd) {
        CassandraJavaUtil.javaFunctions(rdd)
                .writerBuilder("carlocationkeyspace", "average_location_data", CassandraJavaUtil.mapToRow(AverageData.class))
                .saveToCassandra();
    }

 
 
  public static void saveDataToHDFS(final JavaDStream<CarLocation> dataStream, String saveFile, SparkSession sql) {
        System.out.println("Saving to HDFS...");

        dataStream.foreachRDD(rdd -> {
            if (rdd.isEmpty()) {
                return;
            }
            Dataset<Row> dataFrame = sql.createDataFrame(rdd, CarLocation.class);

            // Select and save required columns
            Dataset<Row> dfStore = dataFrame.selectExpr("carid", "latitude", "longitude", "timestamp");
            dfStore.printSchema();
            dfStore.write().mode(SaveMode.Append).parquet(saveFile);
        });
    }
 
 
    public static CarLocation transformData(Row row) {
        System.out.println(row);
        return new CarLocation(row.getString(0), row.getDouble(1), row.getDouble(2)  );
    }

    public static List<AverageData> runBatch(SparkSession sparkSession, String parquetPath, String outputPath) {
        System.out.println("Running Batch Processing");

        // Read data from the parquet file
        Dataset<Row> dataFrame = sparkSession.read().parquet(parquetPath);
        System.out.println(dataFrame);

        // Convert DataFrame to JavaRDD
        JavaRDD<CarLocation> rdd = dataFrame.javaRDD().map(row -> transformData(row));

        // Group data by carid and calculate average for each car
        JavaRDD<AverageData> averageDataRDD = rdd
                .mapToPair(carLocation -> new Tuple2<>(carLocation.getCarid(), carLocation))
                .groupByKey()
                .mapValues((Iterable<CarLocation> carLocations) -> {
                    double totalLatitude = 0;
                    double totalLongitude = 0;
                    long count = 0;

                    for (CarLocation carLocation : carLocations) {
                        totalLatitude += carLocation.getLatitude();
                        totalLongitude += carLocation.getLongitude();
                        count++;
                    }

                    // Calculate averages
                    double avgLatitude = totalLatitude / count;
                    double avgLongitude = totalLongitude / count;

                    return new AverageData(carLocations.iterator().next().getCarid(), avgLatitude, avgLongitude);
                })
                .values();

        // Collect results into a list
        List<AverageData> averageDataList = averageDataRDD.collect();

        // Print the result for each car
        for (AverageData data : averageDataList) {
            System.out.println("Car ID: " + data.getId() + ", Avg Latitude: " + data.getAveragelatitude() + ", Avg Longitude: " + data.getAveragelongitude());
        }

        return averageDataList;
    }


      
}
 