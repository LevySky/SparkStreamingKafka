package com.java.demo;

public class SimpleApp {
//    public static void main(String[] args) {
//        String logFile = "E:/test.txt"; // Should be some file on your system
//        SparkConf conf = new SparkConf().setAppName("sp").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setLogLevel("ERROR");
////
////        JavaRDD<String> logData = sc.textFile(logFile);
////        long numAs = logData.filter(s->s.contains("cf")).count();
////        long numBs = logData.filter(s->s.contains("d")).count();
//
//
//        JavaRDD<String> r1 = sc.parallelize(Arrays.asList(
//                "2018-03-16 18:35:00,f60df5d7-84ab-48c0-b974-c66259527043,OCI,bxq,3119114",
//                "2018-03-16 18:35:00,b4a1593f-fee0-49ea-99f4-3f5aecc0b534,ALT,mjd,8175521"
//        ));
//
//
//        JavaPairRDD<String, String> r2 = r1.flatMap(x -> Arrays.asList(x.toString().split(",")).iterator())
//                .mapToPair(new PairFunction<String, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(String s) throws Exception {
//
//                        return new Tuple2<String, String>(s, "1");
//                    }
//                });
//        //  .mapToPair(x -> new Tuple2<String, String>(x, x.toString()));
//
//
//        System.out.println("Lines with c: " + r2.collect().toString() + ", lines with d: " + r2.first());
//
//        sc.stop();
//    }
}
