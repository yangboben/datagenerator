package io.transwarp.streaming.tester

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util.Properties

import io.transwarp.streaming.tester.StreamSQLTester._
import org.apache.commons.cli.{BasicParser, HelpFormatter, Options}
import org.scalatest.tools.Runner

/**
  * Created by tianming on 15-12-7.
  * wrapper class for org.scalatest.tools.Runner
  */
object TestRunner extends App {
  val options = genOptions()
  val formatter = new HelpFormatter();
  val parser = new BasicParser()
  val cmd = parser.parse(options, args)

  // scalatest runner related options
  val parallel = if (cmd.hasOption("P")) true else false
  val stdout = if (cmd.hasOption("o")) true else false
  val testerDir = cmd.getOptionValue("R")
  if (testerDir == null) {
    formatter.printHelp("tester runner", options)
    System.exit(-1)
  }
  val reporter = cmd.getOptionValue("C")

  // set properties for test cases
  val props = new Properties()
  if (cmd.hasOption("n")) {
    props.setProperty(RUNNER, cmd.getOptionValue("n"))
  }
  if (cmd.hasOption("z")) {
    props.setProperty(ZOOKEEPER, cmd.getOptionValue("z"))
  }
  if (cmd.hasOption("c")) {
    props.setProperty(CLEAN, "true")
  }
  if (cmd.hasOption("h")) {
    props.setProperty(HOST, cmd.getOptionValue("h"))
  }
  if (cmd.hasOption("db")) {
    props.setProperty(DATABASE, cmd.getOptionValue("db"))
  }
  if (cmd.hasOption("t")) {
    props.setProperty(TIMESTAMP, cmd.getOptionValue("t"))
  }
  if (cmd.hasOption("i")) {
    props.setProperty(SUITES, cmd.getOptionValue("i"))
  }
  if (cmd.hasOption("d")) {
    props.setProperty(TESTS_ROOT, cmd.getOptionValue("d"))
  }
  if(cmd.hasOption("p")) {
    props.setProperty(PRINCIPAL, cmd.getOptionValue("p"))
  }
  if(cmd.hasOption("Q")) {
    props.setProperty(PARALLEL_ALL, "true")
  }
  if(cmd.hasOption("O")) {
    props.setProperty(REPORT, cmd.getOptionValue("O"))
  }
  if(cmd.hasOption("G")){
    println(cmd.getOptionValue("G"))
    props.setProperty(GLOBAL_SETTING, cmd.getOptionValue("G"))
  }

  val propertiesFile = new File(PROPS_FILE)
  val out = new OutputStreamWriter(new FileOutputStream(propertiesFile), "UTF-8")
  try {
    props.store(out, "")
  } finally {
    out.close()
  }

  var runnerArgs: Array[String] = Array()
  if (parallel) {
    runnerArgs :+= "-P"
  }
  if (stdout) {
    runnerArgs :+= "-o"
  }
  if(reporter != null) {
    runnerArgs :+= "-C"
    runnerArgs :+= reporter
  }
  runnerArgs :+= "-R"
  runnerArgs :+= testerDir

  val result = Runner.run(runnerArgs)
  if (result) {
    System.exit(0)
  } else {
    System.exit(1)
  }


  def genOptions(): Options = {
    val options = new Options()
    options.addOption("n", true, "runner name")
    options.addOption("c", false, "clean")
    options.addOption("z", "zookeeper", true, "zookeeper server")
    options.addOption("h", "host", true, "inceptor server host")
    options.addOption("db", "database", true, "database")
    options.addOption("P", "parallel-suite", false, "parallel suite")
    options.addOption("Q", "parallel-test", false, "parallel test cases in one suite")
    options.addOption("d", "testsDir", true, "test cases root dir")
    options.addOption("r", "reporter", true, "reporter class")
    options.addOption("o", false, "std output")
    options.addOption("i", "include", true, "suites to be included")
    options.addOption("R", true, "scalatest suite location")
    options.addOption("p", "principal", true, "principal for kerberos")
    options.addOption("C", true, "custom reporter for scalatest")
    options.addOption("O", "output", true, "reporter output file location")
    options.addOption("t", "timestamp", true, "timestamp for kafka data")
    options.addOption("G", "global", true, "global setting sqls which will run for every test case")
  }
}

