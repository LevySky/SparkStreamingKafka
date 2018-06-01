package com.java.storage.logs;

import com.java.streaming.SparkSessionConnection;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LoggerRepository {

    private static Logger log = LoggerFactory.getLogger(LoggerRepository.class);

    public static void save(JavaRDD<String> rdd,String spilteType,String collectionName) {
        //System.out.println("login===============" + rdd.count());
        SparkSessionConnection ssc = SparkSessionConnection.getConn();
        JavaRDD<Document> documents = rdd.map(s -> {
            return DocumentHelper.getByString(s, spilteType);
        });
        // Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", collectionName);
        writeOverrides.put("writeConcern.w", "0");
        WriteConfig writeConfig = WriteConfig.create(ssc.getSparkSession()).withOptions(writeOverrides);
     //   log.info(writeConfig.collectionName());
        MongoSpark.save(documents, writeConfig);
    }
}
