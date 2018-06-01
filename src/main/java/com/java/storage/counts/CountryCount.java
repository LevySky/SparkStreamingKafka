package com.java.storage.counts;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import com.java.streaming.SparkSessionConnection;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class CountryCount {


//    public static void main(String[] args) {
//
//    }
//

    public static void save(JavaRDD<String> rdd){
        System.out.println("count==============="+rdd.count());
        SparkSession sparkSession = SparkSessionConnection.getConn().getSparkSession();
//        ds.printSchema();
//        ds.show();
        Dataset<Row> ds = getDataset(sparkSession,rdd);
        ds.createOrReplaceTempView("country");
        Dataset<Row> res = sparkSession.sql("select server,country,city,sum(number) as num,min(time) as min_time,max(time) as max_time  from country group by server,country,city");
        MongoSpark.write(res).option("collection", "country_count").mode("append").save();//
    }


    public static Dataset<Row> getDataset(SparkSession sparkSession,JavaRDD<String> rdd){

        JavaRDD<Row> data = rdd.map(s->{
            String[] sz = s.split(",");
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            ts = Timestamp.valueOf(sz[0]);
            return RowFactory.create(ts,sz[2],sz[3],sz[4],Long.parseLong(sz[5].trim()),Integer.parseInt(sz[6].trim()));
        });
        List structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("time",DataTypes.TimestampType,true));
        structFields.add(DataTypes.createStructField("user_id",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("country",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("city",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("number",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("server",DataTypes.IntegerType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> ds = sparkSession.createDataFrame(data, structType);
        return ds;
    }


}
