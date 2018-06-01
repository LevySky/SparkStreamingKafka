package com.scala


import com.java.streaming.KafkaOffesetHelper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.java.storage.manager.HandleManager
import com.java.utils.LoggerConfig

import scala.collection.JavaConverters


object StreamManager {


  val conf = new SparkConf().
    setAppName(StreamConstants.appName).
    setMaster(StreamConstants.master).
    //setMaster(StreamConstants.master_mesos).
    set("spark.streaming.backpressure.enabled", "true").
    set("spark.streaming.backpressure.initialRate", "100000").
    set("spark.streaming.kafka.maxRatePerPartition", "5000").
    set("spark.streaming.stopGracefullyOnShutdown", "true") //kill -15 driver_pid
    .set("spark.streaming.kafka.consumer.poll.ms", "60000");
  //   .set("spark.executor.cores","2")
  //.set("spark.executor.cores","2")
  // .set("spark.driver.extraJavaOptions","-Dlog4j.configuration=file:log4j.properties")
  //.set("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintHeapAtGC -XX:+PrintGCTimeStamps")
  val ssc = new StreamingContext(conf, Seconds(StreamConstants.duration))



  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> StreamConstants.brokerlist,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> StreamConstants.groupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  //  def start():Unit  = {
  //
  //    val kafkaParams = initKafkaParams
  //    val manager = new KafkaManager(kafkaParams)
  //    val lines = manager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,Set("part","city"))
  //
  //    println("----------------------"+lines.count())
  //    if(lines.count() != 0){
  //
  //      try{
  //        LoginHandle.handle(lines);
  //      }catch{
  //        case e:Exception=>{
  //          println("Logger handle xception",e.fillInStackTrace())
  //        }
  //      }
  //
  //
  //      lines.foreachRDD(rdd=>{
  //        if(rdd.count() > 0){
  //          manager.updateZKOffsets(rdd);
  //        }
  //      })
  //    }
  //
  //    ssc.start()
  //    ssc.awaitTermination()
  //  }



  /**
    * 消费过从zookeeper中读取偏移量，否则读取kafka保存的
    */
  def initByZK(): Unit = {

    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    if (Kafka10Manager.isAllConsumer()) {
      println(s"消费过, 从zookeeper读取偏移量")
      val f = JavaConverters.mapAsJavaMapConverter(Kafka10Manager.consumerFormZk()).asJava
      val k = JavaConverters.mapAsJavaMapConverter(kafkaParams).asJava
      kafkaStream = KafkaOffesetHelper.create(ssc, k, f)
    } else {
      println("没有消费过，从kafka的存储中读取")
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](StreamConstants.topicsArr, kafkaParams))
    }

    var message = kafkaStream.map(record => (record.key, record.value()))

    try {
      HandleManager.handle(message);
      Kafka10Manager.saveKafkaOffset(kafkaStream)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }



}






