package com.java.storage.logs;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import com.java.streaming.SparkSessionConnection;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.SimpleTimeZone;

public class CountryLog {


    public static void save(JavaRDD<String> rdd) {
        System.out.println("count===============" + rdd.count());
        SparkSessionConnection ssc = SparkSessionConnection.getConn();

        JavaRDD<Document> documents = rdd.map(s -> {
            return getDocumentByString(s);
        });


        // Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "country");
        writeOverrides.put("writeConcern.w", "0");
//        MongoDB支持的WriteConncern选项如下
//
//        w: 数据写入到number个节点才向用客户端确认
//        {w: 0} 对客户端的写入不需要发送任何确认，适用于性能要求高，但不关注正确性的场景
//        {w: 1} 默认的writeConcern，数据写入到Primary就向客户端发送确认
//        {w: “majority”} 数据写入到副本集大多数成员后向客户端发送确认，适用于对数据安全性要求比较高的场景，该选项会降低写入性能
//        j: 写入操作的journal持久化后才向客户端确认
//        默认为”{j: false}，如果要求Primary写入持久化了才向客户端确认，则指定该选项为true
//        wtimeout: 写入超时时间，仅w的值大于1时有效。
//        当指定{w: }时，数据需要成功写入number个节点才算成功，如果写入过程中有节点故障，可能导致这个条件一直不能满足，从而一直不能向客户端发送确认结果，针对这种情况，客户端可设置wtimeout选项来指定超时时间，当写入过程持续超过该时间仍未结束，则认为写入失败。

        WriteConfig writeConfig = WriteConfig.create(ssc.getSparkSession()).withOptions(writeOverrides);
        /*Start Example: Save data from RDD to MongoDB*****************/
        MongoSpark.save(documents, writeConfig);
        /*End Example**************************************************/
        //      jsc.close();
    }

    public static Document getDocumentByString(String str) {

        Document document = new Document();
        try {
            if (StringUtils.isEmpty(str)) {
                return new Document();
            }
            String[] splited = str.split(",");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
            document.put("time", sdf.parse(splited[0]));
            document.put("user_id", splited[2]);
            document.put("country", splited[3]);
            document.put("city", splited[4]);
            document.put("number", Long.parseLong(splited[5].trim()));
            document.put("server", Integer.parseInt(splited[6].trim()));
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            return document;
        }
    }
}
