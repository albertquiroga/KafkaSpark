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
        		rdd.foreachPartition(new VoidFunction<Iterator<String>>(){
        			public void call(Iterator<String> validations) {
        				Configuration config = HBaseConfiguration.create();
						Connection connection = null;
						try {
							connection = ConnectionFactory.createConnection(config);
						} catch (IOException e) {
							e.printStackTrace();
						}
						Table table = null;
						try {
							table = connection.getTable(TableName.valueOf("kafkavalidations"));
						} catch (IOException e) {
							e.printStackTrace();
						}
						
						while(validations.hasNext()){
							JSONObject fullJSON = new JSONObject(validations.next());
							String rowKey = fullJSON.getString("rowkey");
							String timestamp = fullJSON.getString("timestamp");
							
							JSONArray qualifiers = fullJSON.getJSONArray("qualifiers");
							Put put = new Put(rowKey.getBytes());
							
							for(int i=0; i<qualifiers.length(); i++){
								JSONObject qualifier = qualifiers.getJSONObject(i);
								String family = qualifier.getString("family");
								String name = qualifier.getString("name");
								String value = qualifier.getString("value");
								put.addColumn(family.getBytes(), name.getBytes(), value.getBytes());
							}
							
							try {
								table.put(put);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						try {
							table.close();
							connection.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
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
