package io.transwarp.streaming.tester

import java.io.{FileInputStream, InputStreamReader, File}
import java.nio.file.{Path, Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.Eventually
import org.scalatest._
import scala.collection.JavaConversions._

import scala.collection.{immutable, mutable}
import StreamSQLTester._

import scala.io.Source

class StreamSQLTester() extends FunSuite with BeforeAndAfterAll with Eventually{


  val props = loadProps()
  val clean = props.getOrElse(CLEAN, "false").toBoolean
  val host = props.getOrElse(HOST, "localhost")
  val zk = props.getOrElse(ZOOKEEPER, "localhost")
  val principal = {
    val p = props.getOrElse(PRINCIPAL, "")
    if(p.length > 0){
      Option(p)
    }else{
      None
    }
  }
  val runner = props.getOrElse(RUNNER, "")
  val includeFile = if(props.containsKey(SUITES)){
    val include = props.getProperty(SUITES)
    val f = Paths.get(include)
    if(Files.exists(f)) Some(f.toFile) else None
  }else{
    None
  }
  var globalSettings: Array[String] = _
  var testsRoot: Path = _
  var database: String = _
  var timestamp: String = _

  override def nestedSuites = if(props.containsKey(TESTS_ROOT) && props.containsKey(DATABASE) && props.containsKey(TIMESTAMP)) {
    testsRoot = Paths.get(props.getProperty(TESTS_ROOT))
    database = props.getProperty(DATABASE)
    if(runner.size > 0){
      database += s"_${runner}"
    }
    timestamp = props.getProperty(TIMESTAMP)

    val report = props.getOrElse(REPORT, null)
    val allParallel = props.getOrElse(PARALLEL_ALL, "false").toBoolean
    generateSuites(testsRoot, includeFile, report, allParallel)
  }else{
    println("missing config parameters")
    println(TESTS_ROOT+ ":" + props.getOrElse(TESTS_ROOT, ""))
    println(DATABASE + ":" + props.getOrElse(DATABASE, ""))
    Vector[SqlTestSuite]()
  }

  def allSuites(root: Path): Array[File] ={
    val files = root.toFile.listFiles().filter(_.isDirectory)
    val p = files.partition(f => isSuite(f.toPath))
    p._1 ++ p._2.flatMap(f => allSuites(f.toPath))
  }

  def generateSuites(testsRoot: Path, include: Option[File], report: String, parallelTest: Boolean=false): immutable.IndexedSeq[SqlTestSuite] = {
    val suites = include match {
      case Some(file) =>
        val included: mutable.Map[File, Array[String]] = mutable.Map()
        Source.fromFile(file).getLines().filter(_.length > 0).foreach(line => {
          val f = testsRoot.resolve(line).toFile
          if(f.isDirectory){
            if(isSuite(f.toPath)){
              // one test suite
              included += f -> Array()
            }else{
              // add all test suite under this folder
              allSuites(f.toPath).foreach(f => included += f -> Array())
            }
          }else{
            // one test case
            val index = line.lastIndexOf("/")
            val suite = line.substring(0, index)
            val testcase = line.substring(index+1)

            val f = testsRoot.resolve(suite).toFile
            val old = included.getOrElse(f, Array())
            included += f -> (old :+ testcase)
          }

        })
        included.toArray
      case None =>
        allSuites(testsRoot).map(f => (f, Array[String]()))
    }

    suites.map(s => {
      val f = s._1
      val parallel = f.toPath.resolve("p.txt").toFile.exists()
      if(parallel || parallelTest){
        new SqlTestSuiteParallel(this, s._1, s._2, report)
      }else{
        new SqlTestSuite(this, s._1, s._2, report)
      }
    }).toIndexedSeq
  }

  override def beforeAll(): Unit = {
    println("-- before all --")
    val executor = new SqlExecutor(host, "default", principal)
    val sql = s"drop database if exists ${database} cascade;"
    println(sql)
    executor.execQuery(sql)
    val sql2 = s"create database ${database};"
    println(sql2)
    executor.execQuery(sql2)
    executor.closeConnection()

    // load global settings if it exists

    val settings = new File(props.getOrElse(GLOBAL_SETTING, ""))
    globalSettings = if(settings.exists()){
      Source.fromFile(settings).getLines().toArray
    }else{
      Array()
    }
  }

  override def afterAll(): Unit = {
  }

  def isSuite(path: Path): Boolean = {
    val b1 = path.resolve("sql").toFile.exists()
    val b2 = path.resolve("result").toFile.exists()
    b1 && b2
  }
}

object StreamSQLTester {
  def loadProps(): Properties = {
    val propsFile = new File(PROPS_FILE)
    val props = new Properties()
    val input = new InputStreamReader(new FileInputStream(propsFile), "UTF-8")
    try{
      props.load(input)
    } finally {
      input.close()
    }
    props
  }

  val TESTS_ROOT = "tests_root"
  val REPORT = "report"
  val DATABASE = "database"
  val TIMESTAMP = "timestamp"
  val SUITES = "include"
  val HOST = "host"
  val CLEAN = "clean"
  val GLOBAL_SETTING = "global"
  val PRINCIPAL = "principal"
  val RUNNER = "runner"
  val PARALLEL_ALL = "all_parallel"
  val ZOOKEEPER = "zookeeper"
  val PROPS_FILE = "/tmp/tester.properties"
}
