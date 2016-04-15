package io.transwarp.streaming.tester

import java.io.File
import java.util.Properties

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{ProducerRecord, ProducerConfig, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, LogManager}

import scala.io.Source

/**
 * Created by tianming on 15-11-12.
 */
object KafkaUtils {
  def resetOffsets(zookeeper: String, group: String, topic: String): Array[(Int, Int)] = {
    val logger = LogManager.getRootLogger()
    logger.setLevel(Level.ERROR)

    val properties = new Properties()
    properties.put("zookeeper.connect", s"${zookeeper}:2181")
    properties.put("zookeeper.connection.timeout.ms", "1000000")
    properties.put("group.id", group)

    val config = new ConsumerConfig(properties)
    val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs,
                                 config.zkConnectionTimeoutMs, ZKStringSerializer)
    getAndSetOffsets(zkClient, OffsetRequest.EarliestTime, config, topic)
  }

  private def getAndSetOffsets(zkClient: ZkClient, offsetOption: Long, config: ConsumerConfig, topic: String): Array[(Int, Int)] = {
    val partitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, List(topic))
    var partitions: Seq[Int] = Nil

    partitionsPerTopicMap.get(topic) match {
      case Some(l) =>  partitions = l.sortWith((s,t) => s < t)
      case _ => throw new RuntimeException("Can't find topic " + topic)
    }

    var parts: Array[(Int, Int)] = Array()
    var numParts = 0
    for (partition <- partitions) {
      val brokerHostingPartition = ZkUtils.getLeaderForPartition(zkClient, topic, partition)

      val broker = brokerHostingPartition match {
        case Some(b) => b
        case None => throw new KafkaException("Broker " + brokerHostingPartition + " is unavailable. Cannot issue " +
          "getOffsetsBefore request")
      }

      ZkUtils.getBrokerInfo(zkClient, broker) match {
        case Some(brokerInfo) =>
          val consumer = new SimpleConsumer(brokerInfo.host, brokerInfo.port, 10000, 100 * 1024, "UpdateOffsetsInZk")
          val topicAndPartition = TopicAndPartition(topic, partition)
          val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(offsetOption, 1)))
          val offset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
          val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)

          parts :+= (partition, offset.toInt)
          //println("updating partition " + partition + " with new offset: " + offset)
          ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition, offset.toString)
          //numParts += 1
        case None => throw new KafkaException("Broker information for broker id %d does not exist in ZK".format(broker))
      }
    }
    //println("updated the offset for " + numParts + " partitions")
    parts
  }

  def createKafkaProducer(topic: String, brokerlist: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // wait for all in-sync replicas to ack sends
    props.put("request.required.acks", "-1")
    new KafkaProducer[String, String](props)
  }

  // unused
  def writeToProducer(producer: KafkaProducer[String, String], file: File): Unit = {
    val topic = file.getName
    val lines = Source.fromFile(file).getLines()
    lines.foreach(line => {

      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      producer
    })
  }
}
