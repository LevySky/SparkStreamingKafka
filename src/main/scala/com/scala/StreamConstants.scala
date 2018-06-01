package com.scala

object StreamConstants {

  val zkServers = "192.168.119.128:2181,192.168.119.129:2181,192.168.119.130:2181"//"192.168.1.123:2181"
  val zkServerSingle = "192.168.119.128"//"192.168.1.123:2181"
  var master: String = "local[2]"
  var master_mesos: String = "mesos://192.168.119.129:5050"
  val topicsArr = Array("part","city")
  var appName: String = "SparkStreamingLogger"
  var duration: Int = 5
  var durationBatch: Int = 60
  var durationSize: Int = 60
  //"192.168.1.123:9092"
  var brokerlist: String = "192.168.119.128:9092,192.168.119.129:9092,192.168.119.130:9092"
  var groupId: String = "test"
  var checkpoint: String = "E:\\checkpoint"
  var checkpoint4Linux: String = "/home/checkpoint"
}
