package com.java.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkSessionConnection {

    private static volatile SparkSessionConnection conn = null;

    private static JavaSparkContext javaSparkContext;

    private static SparkSession sparkSession;

    private SparkSessionConnection(){
        sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://192.168.1.123:27017/spark.defualt")
                //.config("spark.mongodb.input.database", "spark")
                //.config("spark.mongodb.input.collection", "country")
                .config("spark.mongodb.output.uri", "mongodb://192.168.1.123:27017/spark.defualt")
                //.config("spark.mongodb.output.database", "spark")
                //.config("spark.mongodb.output.collection", "country")
                .getOrCreate();
        this.javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }

    public static SparkSessionConnection getConn(){
        if(conn == null){
            synchronized (SparkSessionConnection.class){
                if(conn == null){
                    conn = new SparkSessionConnection();
                }
            }
        }
        return conn;
    }

    public  JavaSparkContext getJavaSparkContext(){
        return this.javaSparkContext;
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }
}
