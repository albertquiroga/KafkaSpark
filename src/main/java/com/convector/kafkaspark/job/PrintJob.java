package com.convector.kafkaspark.job;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pyro_ on 18/06/2016.
 */
public class PrintJob {

    @SuppressWarnings("serial")
    public static void run() {
        SparkConf conf = new SparkConf().setAppName("Convector").setMaster("spark://master:7077");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("WARN");
        JavaStreamingContext streamingContext = new JavaStreamingContext(context, new Duration(1000));

        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("validations",1);
        JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(streamingContext, "slave1:2181", "convector-consumer-group", topics);

        JavaDStream<String> validations = kafkaStream.map(new Function<Tuple2<String,String>,String>(){
            public String call(Tuple2<String,String> tuple) throws Exception {
                JSONObject json = new JSONObject(tuple._2);
                return json.get("rowkey").toString();
            }
        });
        validations.print();
        //kafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
