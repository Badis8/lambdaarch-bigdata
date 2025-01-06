package tn.enit.bigdata.processor;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import java.util.concurrent.TimeUnit;

import tn.enit.bigdata.entity.AverageData;
import tn.enit.bigdata.entity.CarLocation;
import tn.enit.bigdata.util.PropertyFileReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StreamProcessor {

    public static void main(String[] args) throws Exception {

        String file = "spark-processor.properties";
        Properties prop = PropertyFileReader.readPropertyFile(file);

        SparkConf conf = ProcessorUtils.getSparkConf(prop);

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaSparkContext sc = streamingContext.sparkContext();
        SparkSession spark = SparkSession.builder()
        .config(conf)
        .getOrCreate();
    
 

    

        streamingContext.checkpoint(prop.getProperty("tn.enit.bigdata.spark.checkpoint.dir"));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("tn.enit.bigdata.brokerlist"));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CarLocationDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty("tn.enit.bigdata.topic"));
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop.getProperty("tn.enit.bigdata.resetType"));
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Collection<String> topics = Arrays.asList("sensor-data");

         JavaInputDStream<ConsumerRecord<String, CarLocation>> stream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, CarLocation>Subscribe(topics, kafkaParams));

        JavaDStream<CarLocation> locationStream = stream.map(v -> {
            return v.value();
        });

 
      

  
        String hdfsSavePath = prop.getProperty("tn.enit.bigdata.hdfs.save.path");
        String batchOutputPath = prop.getProperty("tn.enit.bigdata.hdfs.save.output");
        ProcessorUtils.saveTemperatureToCassandra(locationStream);
        ProcessorUtils.saveDataToHDFS(locationStream,hdfsSavePath,spark );
 
        streamingContext.start();
        streamingContext.awaitTermination();

 
/*String hdfsSavePath = prop.getProperty("tn.enit.bigdata.hdfs.save.path");
String batchOutputPath = prop.getProperty("tn.enit.bigdata.hdfs.save.output");
           try {
              System.out.println("Executing batch processing at: " + java.time.LocalDateTime.now());
              List<AverageData> average_data_list= ProcessorUtils.runBatch(spark,hdfsSavePath, batchOutputPath);
              System.out.println("Batch processing completed at: " + java.time.LocalDateTime.now());  
             JavaRDD<AverageData> h = sc.parallelize(average_data_list, 1); // transform to RDD
		     ProcessorUtils.saveAvgToCassandra(h);
          } catch (Exception e) {
              System.out.println("Error occurred during batch processing: " + e.getMessage());
              e.printStackTrace();
          }
      } 
 
       
*/


 
   
    }
}