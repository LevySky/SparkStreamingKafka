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

public class LoginCount {




    public static void save(JavaRDD<String> rdd){
        System.out.println("login==============="+rdd.count());
        SparkSession sparkSession = SparkSessionConnection.getConn().getSparkSession();
//        ds.printSchema();
//        ds.show();
        Dataset<Row> ds = getDataset(sparkSession,rdd);
        ds.createOrReplaceTempView("login");
        Dataset<Row> res = sparkSession.sql("select server,level,count(user_id) as num,min(time) as min_time,max(time) as max_time  from login group by server,level");
        MongoSpark.write(res).option("collection", "login_count").mode("append").save();//
    }

    public static Dataset<Row> getDataset(SparkSession sparkSession,JavaRDD<String> rdd){

        JavaRDD<Row> data = rdd.map(s->{
            String[] sz = s.split(",");
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            ts = Timestamp.valueOf(sz[0]);
            return RowFactory.create(ts,Integer.parseInt(sz[2].trim()),
                    Integer.parseInt(sz[3].trim()),
                    Integer.parseInt(sz[4].trim()),
                    Integer.parseInt(sz[5].trim()),
                    Integer.parseInt(sz[6].trim()),
                    sz[7],sz[8]);
        });
        List structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("time",DataTypes.TimestampType,true));
        structFields.add(DataTypes.createStructField("server",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("user_id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("channel",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("level",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("vip",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("mcc",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("ip",DataTypes.StringType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> ds = sparkSession.createDataFrame(data, structType);
        return ds;
    }


}
