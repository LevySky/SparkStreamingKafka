package com.java.storage.manager;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import com.java.storage.counts.CountryCount;
import com.java.storage.logs.CountryLog;

public class CountryHandle {


    public static void handle(JavaPairInputDStream<String, String> lines){

        JavaPairDStream<String,String> filter = lines.filter(s->s._2().contains("country"));
        JavaDStream<String> logRdd = filter.map(s->s._2());


        JavaDStream<String> countCountryRdd = logRdd.window(Durations.seconds(600),Durations.seconds(600));
        logRdd.foreachRDD(rdd -> {
            CountryLog.save(rdd);
        });
        countCountryRdd.foreachRDD(rdds->{
            CountryCount.save(rdds);
            System.out.println("-------country---count---------------"+ rdds.count());
        });
        System.out.println("----------country---all---------"+ logRdd.count());

    }
}
