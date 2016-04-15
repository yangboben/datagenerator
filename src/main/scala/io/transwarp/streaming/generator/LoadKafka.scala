package io.transwarp.streaming.generator

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, ProducerConfig, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by shentao on 16-3-23.
  */
object LoadKafka {
   def LoadDatatoKafka(topic: String , line :String , producer : KafkaProducer[String, String],partation : Int): Unit ={
           val record = new ProducerRecord[String, String](topic,partation,null,line)
            //val record = new ProducerRecord[String ,String](topic,line)
            producer.send(record)
   }

   def createKafkaProducer(brokerlist: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // wait for all in-sync replicas to ack sends
    props.put("request.required.acks", "-1")
    new KafkaProducer[String, String](props)
  }


}
