package com.java.storage.manager;

import com.java.storage.logs.LoggerRepository;
import com.java.utils.LoggerConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HandleManager {

    private static Logger log = LoggerFactory.getLogger(HandleManager.class);




    public static void handle(JavaPairDStream<String, String> lines){
        for(String key : LoggerConfig.logs.keySet()){
            Map<String,String> log = LoggerConfig.logs.get(key);
            save2DB(lines,log.get("rddFilter"),log.get("strSplite"),log.get("collenctionName"));
        }
    }

    public static void save2DB(JavaPairDStream<String, String> lines,String logType,String spilteType,String collName){


        // JavaDStream<String> countRdd = logRdd.window(Durations.seconds(Constant.durationBatch),Durations.seconds(Constant.durationSize));
        // log.info("{}",logRdd.count());
        try {
            JavaPairDStream<String,String> filter = lines.filter(s->s._2().contains(logType));
            JavaDStream<String> logRdd = filter.map(s->s._2());
            logRdd.foreachRDD(rdd ->{
                        LoggerRepository.save(rdd,spilteType,collName);
                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }

        //        countRdd.foreachRDD(rdds->{
//            LoginCount.save(rdds);
//            System.out.println("-------login---count---------------"+ rdds.count());
//        });
        //  System.out.println("----------login---all---------"+ countRdd.count());

    }


}
