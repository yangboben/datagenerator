package io.transwarp.streaming.generator

import org.apache.commons.cli.Options

import scala.util.Random

/**
  * Created by shentao on 16-3-1.
  */
object GeneratorRunner extends App{

        //val options =genOptions();

        //val producer = LoadKafka.createKafkaProducer(brokerlist)
         Generator.generate(GeneratorScriptParser.parse("test.txt"))

  def genOptions(): Options = {
    val options = new Options()
    options.addOption("n", true, "runner name")
  }
}
