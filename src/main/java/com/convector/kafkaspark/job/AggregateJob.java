package com.convector.kafkaspark.job;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.cartodb.CartoDBClientIF;
import com.cartodb.CartoDBException;
import com.cartodb.impl.ApiKeyCartoDBClient;
import com.google.common.io.Files;

import scala.Tuple2;

public class AggregateJob {

	@SuppressWarnings("serial")
	public static void run() throws Exception {
		SparkConf conf = new SparkConf().setAppName("Convector").setMaster("spark://master:7077");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("WARN");
        JavaStreamingContext streamingContext = new JavaStreamingContext(context, new Duration(10*1000));
        
        streamingContext.checkpoint(Files.createTempDir().getAbsolutePath());

        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("validations",1);
        JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(streamingContext, "slave1:2181", "convector-consumer-group", topics);
        
        JavaPairDStream<String,Integer> estacions = kafkaStream.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){
        	public Tuple2<String,Integer> call(Tuple2<String,String> in){
        		JSONObject fullJSON = new JSONObject(in._2);
        		JSONArray array = fullJSON.getJSONArray("qualifiers");
        		JSONObject estacio = array.getJSONObject(1);
        		return new Tuple2<String,Integer>(estacio.getString("value"),1);
        	}
        });
        
        JavaPairDStream<String,Integer> counts = estacions.reduceByKeyAndWindow(new Function2<Integer,Integer,Integer>(){
        	public Integer call(Integer i1, Integer i2){
        		return i1+i2;
        	}
        }, new Duration(60*5*1000), new Duration(10*1000));
        
        counts.print();
        
        counts.foreachRDD(new Function<JavaPairRDD<String,Integer>,Void>(){
        	public Void call(JavaPairRDD<String,Integer> rdd) throws Exception {
        		CartoDBClientIF cartoClient = new ApiKeyCartoDBClient("cteixidogalvez", "7977f5eaebeb6279ca4b9224a1f2988f14ee1824");
        		cartoClient.executeQuery("DELETE FROM validacions_metro_online;");
        		rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>(){
					public void call(Iterator<Tuple2<String, Integer>> agregats) throws Exception {
						Timestamp t = new Timestamp(System.currentTimeMillis());
						CartoDBClientIF cartoDBCLient = null;
						try{
							cartoDBCLient = new ApiKeyCartoDBClient("cteixidogalvez", "7977f5eaebeb6279ca4b9224a1f2988f14ee1824");
						} catch (Exception e){
							e.printStackTrace();
						}
						while(agregats.hasNext()){
							Tuple2<String,Integer> tuple = agregats.next();
							String subQuery = "(the_geom,codi_estacio,instant_pas,validacions) VALUES("
									+ "ST_SetSRID(ST_Point(" + Coordinater.getCoordinates(tuple._1) + "),4326),"
									+ tuple._1() + ","
									+ "TO_TIMESTAMP('" + t.toString() + "','YYYY-MM-DD HH24:MI:SS.MS'),"
									+ tuple._2().toString() + ");";
							try{
								cartoDBCLient.executeQuery("INSERT INTO validacions_metro_online " 			+ subQuery);
								cartoDBCLient.executeQuery("INSERT INTO validacions_metro_online_animacio " + subQuery);
							} catch (Exception e){
								e.printStackTrace();
							}
						}
					}
        		});
        		return null;
        	}
        });
        
        streamingContext.start();
        streamingContext.awaitTermination();
        
	}
	
}
