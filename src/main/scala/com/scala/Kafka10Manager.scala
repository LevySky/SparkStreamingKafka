package com.scala

import com.java.streaming.KafkaOffesetHelper
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

//import org.apache.spark.streaming.kafka010
object Kafka10Manager{

  import com.java.utils.MyZkSerializer


  val zkUtils = ZkUtils.apply(StreamConstants.zkServers, 60000, 60000, false);
  val zkClient = new ZkClient(StreamConstants.zkServers, 60000, 60000)
  zkClient.setZkSerializer(new MyZkSerializer)


  def isAllConsumer(): Boolean = {
    var children = 0
    var _topicDirs: ZKGroupTopicDirs = null
    for (topic <- StreamConstants.topicsArr) {
      _topicDirs = new ZKGroupTopicDirs(StreamConstants.groupId, topic)
      if (_topicDirs != null) {
        children += zkClient.countChildren(s"${_topicDirs.consumerOffsetDir}")
      }
    }
    return children > 0
  }

  def consumerFormZk(): Map[TopicPartition, java.lang.Long] = {
    var finalFromOffsets: Map[TopicPartition, java.lang.Long] = Map()
    var children: Int = -1
    var _topicDirs: ZKGroupTopicDirs = null
    for (topic <- StreamConstants.topicsArr) {
      _topicDirs = new ZKGroupTopicDirs(StreamConstants.groupId, topic)
      children = zkClient.countChildren(s"${_topicDirs.consumerOffsetDir}")

      if (children > -1) {
        var fromOffsets: Map[TopicAndPartition, Long] = Map()
        //---get partition leader begin----
        val req = new TopicMetadataRequest(List(topic), 0) //得到该topic的一些信息，比如broker,partition分布情况
        val getLeaderConsumer = new SimpleConsumer(StreamConstants.zkServerSingle, 9092, 60000, 64 * 1024, "leaderLookup") // low level api interface
        val res = getLeaderConsumer.send(req) //TopicMetadataRequest   topic broker partition 的一些信息
        val topicMetaOption = res.topicsMetadata.headOption
        val partitions = topicMetaOption match {
          case Some(tm) => tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
          case None => Map[Int, String]()
        }
        println(partitions) //--get partition leader  end----
        for (i <- 0 until children) {
          println(s"从zookeeper读取偏移量   ${_topicDirs.consumerOffsetDir}/${i}")
          val partitionOffset = zkClient.readData[String](s"${_topicDirs.consumerOffsetDir}/${i}")
          val tp = TopicAndPartition(topic, i)
          //---additional begin-----
          val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1))) // -2,1
          val consumerMin = new SimpleConsumer(partitions(i), 9092, 60000, 1000000, "getMinOffset")
          val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
          var nextOffset = partitionOffset.toLong

          if (curOffsets.length > 0 && nextOffset < curOffsets.head) { //如果下一个offset小于当前的offset
            println(partitionOffset,curOffsets)
            nextOffset = curOffsets.head
          } //---additional end-----
          fromOffsets += (tp -> nextOffset)

          consumerMin.close()
        }
        fromOffsets.foreach(offset => {
          finalFromOffsets += (new TopicPartition(offset._1.topic, offset._1.partition) -> offset._2)
        })

        getLeaderConsumer.close()

      }
    }
    println(finalFromOffsets)
    return finalFromOffsets
  }

  def saveKafkaOffset(kafkaStream: InputDStream[ConsumerRecord[String, String]]): Unit ={
    kafkaStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (or <- offsetRanges) {
        val zkPath = s"/consumers/${StreamConstants.groupId}/offsets/${or.topic}/${or.partition}"
        println(s"${zkPath}  ${or.topic} ${or.partition} ${or.fromOffset} ${or.untilOffset}") //println(rdd.count())
        zkUtils.updatePersistentPath(zkPath, or.fromOffset.toString, ZkUtils.DefaultAcls(true))
        KafkaOffesetHelper.save(or.untilOffset, or.fromOffset, or.topic, or.partition, rdd.count()); //        System.out.println(zkUtils.getPartitionsForTopics(Seq("city","part","login")))
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

        println()

        //println(or)
      }
    })
  }
}
