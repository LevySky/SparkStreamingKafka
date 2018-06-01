//package com.spark.streaming;
//
//import com.scala.MyScalaTest;
//import com.spark.storage.utils.KafkaOffesetHelper;
//import com.spark.streaming.handle.CountryHandle;
//import com.spark.streaming.handle.LoginHandle;
//import kafka.serializer.StringDecoder;
//import org.apache.spark.Accumulator;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka.OffsetRange;
//
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.nio.file.StandardOpenOption;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicReference;
//
//public class StremHandle {
//
//    public static void handle() throws InterruptedException {
//
//        SparkConf sparkConf = new SparkConf().setAppName(Constant.appName).setMaster(Constant.master);
//
//        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Constant.duration));
//     //   jssc.checkpoint(Constant.checkpoint); //设置检查点
//        jssc.checkpoint(Constant.checkpoint4Linux); //设置检查点
//
//        Map<String, String> kafkaParams = new HashMap<String, String>();
//        kafkaParams.put("metadata.broker.list",Constant.brokerlist);
//        kafkaParams.put("group.id",Constant.groupId);
//
//        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class,
//                String.class, StringDecoder.class,
//                StringDecoder.class, kafkaParams,
//                Constant.topics);
//
//        Accumulator<Integer> acc = jssc.sparkContext().accumulator(0);
//
//        AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
//
//        JavaDStream<String> batchs = lines.transformToPair(
//                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
//                    @Override
//                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
//                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                        offsetRanges.set(offsets);
//                        acc.add(1);
//                        return rdd;
//                    }
//                }
//        ).map(s -> s._2());
//
//
//        batchs.foreachRDD(r -> {
//            for (OffsetRange offsetRange : offsetRanges.get()) {
//
//                MyScalaTest.insertTest(offsetRange.untilOffset(),offsetRange.topic(),offsetRange.partition(),r.count());
//                System.out.println("-------MyScalaTest-----------");
//                KafkaOffesetHelper.save(offsetRange.untilOffset(),offsetRange.topic(),offsetRange.partition(),r.count());
//                System.out.println("update kafka_offsets set offset ='"
//                        + offsetRange.untilOffset() + "'  where topic='"
//                        + offsetRange.topic() + "' and partition='"
//                        + offsetRange.partition() + "'");
//
//            }
//        });
//
////        CountryHandle.handle(lines);
////        LoginHandle.handle(lines);
//
//        System.out.println(acc.value() + "----------kafka--bantch--all---------" + batchs.count());
//
//        jssc.start();
//        jssc.awaitTermination();
//    }
//
//
//    private static void wirteFile(JavaRDD<String> line) {
//        List<String> list = line.collect();
//
//        list.forEach(s -> {
//            try {
//                String str = s.toString();
//                if ("(null".equals(str) || ")".equals(str)) {
//                    System.out.println("----" + str);
//                } else {
//                    Files.write(Paths.get("E:/SqlTest.txt"), str.getBytes(), StandardOpenOption.APPEND);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//    }
//
//
//}
