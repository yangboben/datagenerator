package io.transwarp.streaming.tester

import java.io.File
import org.apache.commons.io.FileUtils
import org.scalatest.Informer

import scala.collection.mutable
import scala.collection.JavaConversions._

import scala.io.Source
import scala.sys.process._
import SqlTestCase._

import scala.util.Try

/**
 * Created by tianming on 15-10-21.
 */

trait Result
case object Pass extends Result
case object LineCountDiff extends Result
case class LinesNotMatch(diffs: Array[(String, String)]) extends Result
case class LinesInclude(output: Array[String], target: Array[String]) extends Result
case class LinesExclude(output: Array[String], target: Array[String]) extends Result
case class DataReceived() extends Result
case class DataNotReceived() extends Result

class SqlTestCase(suite: SqlTestSuite, tester: StreamSQLTester, val name: String, informer: Option[Informer], useJdbc: Boolean=true) {
  val host = tester.host
  val zk = tester.zk
  val database = tester.database
  val timestamp = tester.timestamp
  val principal = tester.principal

  val p = suite.suite.toPath
  val file = p.resolve(s"sql/${name}.sql").toFile
  val resultFile = p.resolve(s"result/${name}.txt").toFile

  val suiteName = suite.suiteName
  var generatedSQL: Array[String] = Array(s"use ${database};")

  var totalTimeLimit: Int = 30
  var maxRetry: Int = -1
  var streamIds: Array[String] = _
  var tableIds: Set[String] = _
  var externalTableIds: Set[String] = _
  var viewIds: Array[String] = _

  var variables = new mutable.HashMap[String, String]()
  variables.put(TABLE_PREFIX, s"${suiteName}_${name}_")
  variables.put(TOPIC_PREFIX, s"${timestamp}.${suiteName}")
  variables.put(DATABASE, database)
  variables.put(SUITE, suite.suite.getAbsolutePath)
  val defaultApp = s"${suiteName}_${name}"
  variables.put(APP, defaultApp)

  var isPassed = true

  var stepIndex = 0
  var targetCount = 0
  val sqls = SqlScriptParser.parse(this, file)
  val apps = getApps(sqls.map(_._1)) + defaultApp
  val targets = readTargets()
  val executor = new SqlExecutor(host, database, principal)

  def getApps(sql: Array[String]): Set[String] = {
    val p = "use application (.+);".r
    var apps: Set[String] = Set()
    sqls.foreach(sql => {
      replaceVar(sql._1) match {
        case p(app) => {
          apps += app
        }
        case _ => {}
      }
    })
    apps
  }

  def readTargets(): Array[Array[String]] = {
    var targets: Array[Array[String]] = Array()
    val lines = Source.fromFile(resultFile).getLines().map(line => replaceVar(line)).toArray

    var target: Array[String] = Array()
    var opening: Boolean = false
    lines.foreach(line => {
      if(line.startsWith("###")){
        if(line.indexOf(TARGET.START) > 0) {
          opening = true
          target = Array()
        }
        if(line.indexOf(TARGET.END) > 0) {
          opening = false
          targets :+= target
        }
      }else{
        if(opening){
          target :+= line
        }
      }
    })

    // if only have one target without using ### comment
    if(targets.isEmpty) {
      targets :+= lines
    }
    targets
  }

  def resetOffset(table: String, topic: String): Unit = {
    val group = getGroup(table)
    printMessage(s"reset topic: ${topic}, group: ${group}, zk: ${zk}")
    try{
      val parts = KafkaUtils.resetOffsets(zk, group, topic)
      parts.foreach(p => printMessage(s"KAFKA: updating partition ${p._1} with new offset: ${p._2}"))
      printMessage(s"KAFKA: updated the offset for ${parts.length} partitions")
    }catch{
      case e: Throwable =>
        e.printStackTrace()
    }
  }

  def before(): Unit = {
    tableIds = Set()
    externalTableIds = Set()
    streamIds = Array()
    viewIds = Array()

    apps.foreach(app => {
      runOp(s"create application if not exists ${app};", None)
      runOp(s"use application ${app};", None)
      stopSteam("all")
    })
    tester.globalSettings.foreach(sql => runOp(sql))
  }

  def after(): Unit = {
    val timeout = 30
    // left sqls might contain clean sqls
    for(i <- (stepIndex+1) until sqls.length) {
      val op = sqls(i)
      runOp(op._1, op._2)
    }

    val stopStreams = true
    val dropTables = false
    if(stopStreams){
      apps.foreach(app => {
        runOp(s"use application ${app};", timeout=timeout)
        stopSteam("all")
        runOp(s"drop application ${app} cascade;", timeout=timeout)
      })
    }

    if(dropTables){
      tableIds.foreach( id => {
        runOp(s"drop table if exists ${id};", timeout=timeout)
      })

      viewIds.foreach( id => {
        runOp(s"drop view if exists ${id};", timeout=timeout)
      })
    }

    if(useJdbc) {
      executor.closeConnection()
    }
  }

  def stopSteam(id: String): Unit = {
    runOp(s"stop streamjob ${id};", None, timeout=30)
  }

  def printMessage(message: String): Unit = {
    informer match {
      case Some(i) => i(message)
      case None => println(s"[${suiteName}/${file.getName}]: ${message}")
    }
  }

  def runOp(op: String, comment: Option[String] = None, timeout: Int= -1, record: Boolean=true): Either[Array[String], Throwable] = {
    val sql = replaceVar(op)
    printMessage(sql)
    if(record){
      val tokens = sql.split(";").filter(_.length > 0).map(_ + ";")
      generatedSQL ++= tokens
    }

    val ops = OpFactory.buildOps(sql, comment.map(c => replaceVar(c)))
    ops.foreach(_.pre(this))
    val res = executor.execQuery(sql, timeout)
    res match {
      case Left(outputs) =>
        if(!outputs.isEmpty){
          printMessage(s"OUTPUT:\t${outputs.mkString("\n\t")}\n")
        }
      case Right(e) => printMessage(s"ERROR: ${e.getMessage}")
    }
    ops.foreach(_.post(this, res))
    res
  }

  def replaceVar(cmd: String): String ={
    val pattern = """\$\{(\w+)\}""".r
    pattern.replaceAllIn(cmd, _ match { case pattern(key) => variables.getOrElse(key, "\\${"+key+"}")})
  }

  /**
   * it might be called multiple times by tester
 *
   * @return
   */
  def run(): Option[String] = {
    var error: Option[String] = None
    for(i <- stepIndex until sqls.length) {
      stepIndex = i
      val op = sqls(i)
      val _op = op._2.getOrElse("")
      if(_op.contains(OP.TARGET) || _op.contains(OP.ERROR)){
        val result = runOp(op._1, op._2, totalTimeLimit, record=if(isPassed) true else false)
        val target = targets(targetCount)

        if(_op.contains(OP.TARGET)) {
          result match {
            case Left(output) =>
              val res = verify(output, target)
              error = getErrorMsg(output, target, res)
              res match {
                case Pass =>
                  printMessage(Console.GREEN + s"##### test case $targetCount pass #####" + Console.RESET)
                  targetCount += 1
                case _ => {}
              }

            case Right(e) =>
              error = Some(s"Something wrong: ${e}")
          }
          if (error.isDefined) {
            isPassed = false
            return error
          }
        }else if(_op.contains(OP.ERROR)){
          result match {
            case Left(output) =>
              error = Some(s"Something wrong: ${output}")

            case Right(e) =>
              val msg = Array(e.getMessage)
              val res = verify(msg, target)
              error = getErrorMsg(msg, target, res)
              res match {
                case Pass =>
                  printMessage(Console.GREEN + s"##### test case $targetCount pass #####" + Console.RESET)
                  targetCount += 1
                case _ => {}
              }
          }
          if (error.isDefined) {
            isPassed = false
            return error
          }
        }
      }else{
        runOp(op._1, op._2)
      }
    }
    isPassed = true

    error
  }

  def getErrorMsg(output: Array[String], target: Array[String], result: Result): Option[String] = {
    def genMsg(comment: String, output: Array[String], target: Array[String]): String = {
      s"""
         |failed at ${targetCount}th target
         |${comment}
         |== output ==
         |${output.mkString("\n")}
         |
         |== expect ==
         |${target.mkString("\n")}
       """.stripMargin
    }
    result match {
      case Pass =>
        None
      case LineCountDiff =>
        Some(genMsg("line number does not match", output, target))
      case LinesNotMatch(diffs) =>
        Some(genMsg("some lines not match, list below", diffs.map(_._1), diffs.map(_._2)))
      case LinesInclude(_, _) =>
        Some(genMsg("", output, target))
      case LinesExclude(_, _) =>
        Some(genMsg("", output, target))
      case DataReceived() =>
        None
      case DataNotReceived() =>
        Some("data has not all received")
    }
  }
  /**
   *
   * @param res
   * @param target
   * @return
   */
  def verify(res: Array[String], target: Array[String], separator: String = "\\s+"): Result = {
    // support for only comparing how many rows

    var pattern = TARGET.MATCH
    var topic = ""
    var tableName = ""
    if(target.length > 0 && target(0).startsWith("#")) {
      val firstLine = target(0)
      val rowsIndex = firstLine.indexOf(TARGET.ROWS)
      if(rowsIndex > 0) {
        val rows = firstLine.substring(rowsIndex + TARGET.ROWS.length).trim.toInt
        if(res.length == rows){
          return Pass
        }
        else{
          return LineCountDiff
        }
      }

      if(firstLine.indexOf(TARGET.INCLUDE) > 0) {
        pattern = TARGET.INCLUDE
      }
      if(firstLine.indexOf(TARGET.EXCLUDE) > 0) {
        pattern = TARGET.EXCLUDE
      }
      if(firstLine.indexOf(TARGET.DATA_RECEIVED) > 0) {
        val splits = replaceVar(firstLine).split("\\s+")
        pattern = TARGET.DATA_RECEIVED
        topic = splits(2)
        tableName = splits(3)
      }
    }

    pattern match {
      case TARGET.INCLUDE =>
        contain(res, target.slice(1, target.length), separator)
      case TARGET.EXCLUDE =>
        contain(res, target.slice(1, target.length), separator, reverse=true)
      case TARGET.MATCH =>
        matchExactly(res, target, separator)
      case TARGET.DATA_RECEIVED => {
        checkDataReceived (topic, getGroup(tableName))
      }
    }
  }

  def matchExactly(res: Array[String], target: Array[String], separator: String="\\s+"): Result = {
    var diffs: Array[(String, String)] = Array()

    if(res.length != target.length) {
      return LineCountDiff
    }

    (res zip target).foreach( t => {
      val (resultLine, targetLine) = t
      if(!matchLine(resultLine, targetLine, separator)){
        diffs :+= (resultLine, targetLine)
      }
    })

    if(diffs.length == 0){
      Pass
    }else{
      LinesNotMatch(diffs)
    }
  }

  def checkDataReceived(topic: String, groupId: String) : Result = {
    var errors = Array[String]()
    val command = Seq("/usr/lib/kafka/bin/kafka-run-class.sh",
      "kafka.tools.ConsumerOffsetChecker",
      "--group",
      groupId,
      "--topic",
      topic)
    println(command)
    val lines = command lines_! ProcessLogger(line => {errors = errors :+ line})
    lines.foreach(println(_))

    val lags = lines.filter(!_.contains("logSize")).map(_.split("\\s+")(5)).toArray
    println("lags:" + lags.toSeq)
    val allDone = lags.size > 0 && lags.forall(_.toLong == 0L)

    if (errors.length > 0 ) {
      errors.foreach(println(_))
    }
    println("allDone:" + allDone)
    if (allDone) {
      return Pass
    } else {
      return DataNotReceived()
    }
  }

  /**
   *
   * @param res
   * @param target
   * @param separator
   * @param reverse true: test if include target, false: test if exclude target
   * @return
   */
  def contain(res: Array[String], target: Array[String], separator: String="\\s+", reverse: Boolean=false): Result = {
    var count = 0
    var i = 0
    for(line <- res){
      if(i < target.length) {
        val targetLine = target(i)
        if(matchLine(line, targetLine, separator)){
          count += 1
          i += 1
        }
      }
    }
    if(!reverse) {
      if(count == target.length){
        Pass
      }else{
        LinesInclude(res, target)
      }
    }else {
      if(count == 0){
        Pass
      }else{
        LinesExclude(res, target)
      }
    }
  }

  def matchLine(line: String, target: String, separator: String): Boolean = {
    val resultTokens = line.split(separator).map(_.trim).filter(_.length > 0)
    val targetTokens = target.split(separator).map(_.trim).filter(_.length > 0)

    // for different formatted empty line
    val n1 = if(resultTokens.length == 0) 0 else resultTokens.map(_.length).sum
    val n2 = if(targetTokens.length == 0) 0 else targetTokens.map(_.length).sum
    if(n1 == 0 && n2 == 0) {
      return true
    }

    if(resultTokens.length != targetTokens.length){
      return false
    }

    (resultTokens zip targetTokens).foreach( words => {
      val (word, reg) = words
      try{
        if(word != reg && !word.matches(reg)){
          return false
        }
      }catch{
        // avoid exception when reg is not a valid regex expression
        case _: Throwable => { return false }
      }
    })
    true
  }

  def getTopic(sql: String): Option[String] = {
    val p = "['\"]topic['\"]=['\"](.+?)['\"]".r
    p.findFirstIn(sql.toLowerCase()) match {
      case Some(s) => s match {
        case p(topic) => Some(topic)
        case _ => None
      }
      case _ => None
    }
  }

  def getGroup(table: String): String = {
    s"${variables.get(APP).get}-${database}.${table}"
  }

  def toFile(filename: String): Unit ={
    FileUtils.writeStringToFile(new File(filename), generatedSQL.mkString("\n"))
  }

//  def getRunTime(): Option[Double] = {
//    val line = errors.last
//    if(line.startsWith(TIME)){
//      Some(line.split(" ")(2).toDouble)
//    }else{
//      None
//    }
//  }

}

object TARGET {
  val ROWS = "rows"
  val MATCH = "match"
  val INCLUDE = "include"
  val EXCLUDE = "exclude"
  val DATA_RECEIVED = "data-received"

  val START="start"
  val END="end"
}



object SqlTestCase {
  val TABLE_PREFIX = "prefix"
  val TOPIC_PREFIX = "db"
  val DATABASE = "database"
  val APP = "app"
  val SUITE = "suite"
}
