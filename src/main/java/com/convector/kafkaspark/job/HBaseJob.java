package com.convector.kafkaspark.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.JavaRDD;
import org.json.JSONArray;
import org.json.JSONObject;

import scala.Tuple2;

public class HBaseJob {

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
                return new JSONObject(tuple._2).toString();
            }
        });
        
        validations.foreachRDD(new Function<JavaRDD<String>,Void>(){
        	public Void call(JavaRDD<String> rdd) {
        		rdd.foreach(new VoidFunction<String>() {
        			public void call(String s) {
        				Configuration config = HBaseConfiguration.create();
        				Connection connection;
        				Table table = null;
						try {
							connection = ConnectionFactory.createConnection(config);
							table = connection.getTable(TableName.valueOf("kafkavalidations"));
						} catch (IOException e1) {
							e1.printStackTrace();
						}
        				
        				JSONObject fullJson = new JSONObject(s);
        				Put put = new Put(fullJson.get("rowkey").toString().getBytes());
        				JSONArray qualifiers = fullJson.getJSONArray("qualifiers");
        				String rowkey = fullJson.getString("rowkey");
        				String timestamp = fullJson.getString("timestamp");
        				String out = rowkey + "||" + timestamp + "||";
        				for(int i = 0; i<qualifiers.length(); i++) {
        					JSONObject qualifier = qualifiers.getJSONObject(i);
        					put.addColumn(qualifier.getString("family").getBytes(), qualifier.getString("name").getBytes(), Long.parseLong(timestamp), qualifier.getString("value").getBytes());
        					out.concat(qualifier.getString("family")+":"+qualifier.getString("name")+":"+qualifier.getString("value"));
        				}
        				System.out.println(out);
        				try {
        		            table.put(put);
        		        } catch (IOException e) {
        		            e.printStackTrace();
        		        }
        			}
        		});
        		return null;
        	}
        });;
        
        validations.print();
        
        streamingContext.start();
        streamingContext.awaitTermination();
        
	}
}
