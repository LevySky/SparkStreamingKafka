package com.java.streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class KafkaOffesetHelper {

    public static InputDStream<ConsumerRecord<String, String>> create(StreamingContext streamingContext, Map<String, Object> kafkaParams, Map fromOffsets){
        InputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets)
        );
        return stream;
    }


    public static void save(long offset,long lastOffset,String topic,Integer part,long dataSize) {
        try {
            SparkSessionConnection ssc = SparkSessionConnection.getConn();
            JavaRDD<Document> documents = ssc.getJavaSparkContext().parallelize(Arrays.asList(getDocumentByX(offset,lastOffset,topic,part,dataSize)));
            Map<String, String> writeOverrides = new HashMap<String, String>();
            writeOverrides.put("collection", "kafka_offset");
            writeOverrides.put("writeConcern.w", "1");
            //  System.out.println(ssc.getSparkSession().conf().getAll());
            // System.out.println(jsc.getConf().getOption("spark.mongodb.input.uri"));
            //jsc.getConf().set();
            //  System.out.println(writeOverrides);
            // WriteConcernConfig wcc = WriteConcernConfig.create(ssc.getSparkSession()).withOptions(writeOverrides);
            WriteConfig writeConfig = WriteConfig.create(ssc.getJavaSparkContext()).withOptions(writeOverrides);
            MongoSpark.save(documents,writeConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Document getDocumentByX(long offset,long lastOffset,String topic,Integer part,long dataSize) {
        Document document = new Document();
        try {
            document.put("update_time",new Date());
            document.put("offset", offset);
            document.put("last_offset",lastOffset);
            document.put("topic",topic);
            document.put("partition",part);
            document.put("data_size",dataSize);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            return document;
        }
    }
}
