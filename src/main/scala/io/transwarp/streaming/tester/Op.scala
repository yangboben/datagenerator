package io.transwarp.streaming.tester

import java.io.File
import sys.process._

import org.apache.spark.performance.LogAnalyser

/**
 * Created by tianming on 15-11-12.
 */

/**
  * you can add new op by extending BaseOp and add new entry in {@link OpFactory#buildOps buildOps}
  * every sql statement can have multiple ops,
  * before executing the sql stmt, op's pre() will run. And then run post() after the stmt
  * there are no running orders among ops.
  */
class BaseOp {
  def pre(testcase: SqlTestCase): Unit = {}
  def post(testcase: SqlTestCase, result: Either[Array[String], Throwable]): Unit = {}
}

class CreateTable(val sql: String, isExternal: Boolean=false) extends BaseOp{
  var name: String = _
  override def pre(testcase: SqlTestCase): Unit = {
    name = OpFactory.getCreateOpId(sql, if(isExternal) 1 else 0)
    if(isExternal){
      testcase.externalTableIds += name

    }else{
      testcase.tableIds += name
    }
  }
  override def post(testcase: SqlTestCase, result: Either[Array[String], Throwable]): Unit = {
    if(isExternal){
      testcase.executor.execQuery(s"delete from ${name}")
    }
  }
}

class CreateView(val sql: String) extends BaseOp {
  override def pre(testcase: SqlTestCase): Unit = {
    testcase.viewIds :+= OpFactory.getCreateOpId(sql)
  }
}

class CreateStream(val sql: String, reset: Boolean=true) extends BaseOp {
  override def pre(testcase: SqlTestCase): Unit = {
    val name = OpFactory.getCreateOpId(sql)
    testcase.tableIds += name
    if(reset){
      val topic = testcase.getTopic(sql)
      topic.map(testcase.resetOffset(name, _))
    }
  }
}

class StartStream(val sql: String) extends BaseOp {
  override def pre(testcase: SqlTestCase): Unit = {
    val key = s"stream${testcase.streamIds.length+1}"
    //val streamId = UUID.randomUUID().toString.replace("-", "")
    val streamId = s"${testcase.variables.get(SqlTestCase.TABLE_PREFIX).get}${key}"
    testcase.streamIds :+= streamId
    testcase.variables.put(key, streamId)
    testcase.executor.execQuery(s"set stream.id=${streamId};")
  }
}

class Target(val sql: String) extends BaseOp {

}

class Error(val sql: String) extends BaseOp {

}

class Analyse(streamid: String) extends BaseOp {
  override def post(testcase: SqlTestCase, result: Either[Array[String], Throwable]): Unit = {
    println(s"analyse streamid: ${streamid}")
    val analyser = new LogAnalyser(testcase)
    val host = testcase.host
    println(host)
    val src = s"root@${host}://tmp/logging/streamsql/*"
    val dest = s"/tmp/logging/streamsql/"
    new File(dest).mkdirs()
    analyser.copyReportFiles(src, dest)
    val firstTow = analyser.getLatestReportPath(dest, streamid).toArray
    println(firstTow(0))
    analyser.analyseOneReport(firstTow(0))
  }
}

class TimeLimit(val timeout: Int) extends BaseOp {
  override def pre(testCase: SqlTestCase): Unit = {
    testCase.printMessage(s"set time limit to ${timeout}s")
    testCase.totalTimeLimit = timeout
  }
}

/**
  * use to run external command
  * @param cmd
  */
class Cmd(val cmd: Seq[String]) extends BaseOp {
  override def pre(testcase: SqlTestCase): Unit = {
    testcase.printMessage(s"run cmd: ${cmd}")
    cmd !
  }
}

class UseApp(val name: String) extends BaseOp {
  override def pre(testcase: SqlTestCase): Unit = {
    testcase.variables.update(SqlTestCase.APP, name)
  }
}

/**
  * it is used to generate different ops from sql and comments
  */
object OpFactory{
  val TABLE = "table"
  val EXTERNAL = "external"
  val VIEW = "view"
  val STREAM = "stream"

  val DOUBLE_QUOTE = "\""
  val QUOTE = "'"
  val SPACE = " "
  val APP = "application"

  def isQuote(token: String) : Boolean =
    ( token.startsWith(DOUBLE_QUOTE) && token.endsWith(DOUBLE_QUOTE) ) || ( token.startsWith(QUOTE) && token.endsWith(QUOTE) )

  def toTokens(sql:String) =
    sql.split("\\s+").map(token => token.trim)

  def parseExternalCmd(cmd: String): Seq[String] ={
    var cmdSeq: Seq[String] = Seq()

    var open = false
    var token = ""
    cmd.foreach(v => {
      val c = v.toString
      if(open){
        if(c == SPACE){
          val v1 = token(0).toString
          if(v1 == QUOTE ||  v1== DOUBLE_QUOTE){
            //space is part of cmd token
            token = token + c
          }else{
            cmdSeq :+= token
            token = ""
            open = false
          }
        }else{
          val v1 = token(0).toString
          if((v1 == QUOTE ||  v1 == DOUBLE_QUOTE) && v1 == c){
            // close
            cmdSeq :+= token.substring(1, token.length)
            token = ""
            open = false
          }else{
            token = token + c
          }
        }
      }else{
        if(c != SPACE){
          open = true
          token = token + c
        }
      }
    })
    if(token != ""){
      cmdSeq :+= token
    }
    cmdSeq
  }

  // build operators according to sql and comment
  def buildOps(sql: String, comment: Option[String]): Array[BaseOp] = {
    var ops: Array[BaseOp] = Array()
    val tokens = toTokens(sql)
    if(tokens.length >= 2) {
      val t1 = tokens(0).toLowerCase
      var t2 = tokens(1).toLowerCase
      if(t2 == "if"){
        t2 = tokens(4).toLowerCase // skip if
      }
      t1 match {
        case OP.CREATE =>
          createOp(t2, sql, comment).map(ops :+= _)
        case OP.USE =>
          useOp(t2, sql, comment).map(ops :+= _)
        case _ => {}
      }
    }

    // one comment can only have one option pair now
    comment.map(token => {
      val p = token.split(":").map(_.trim)
      p(0).toLowerCase match {
        case OP.STREAM =>
          ops :+= new StartStream(sql)
        case OP.TIME_LIMIT =>
          ops :+= new TimeLimit(p(1).toInt)
        case OP.ANALYSE =>
          println(s"add analyse ${p(1)}")
          ops :+= new Analyse(p(1))
        case OP.CMD =>
          val cmd = p(1).trim
          val cmdSeq = parseExternalCmd(cmd)
          ops :+= new Cmd(cmdSeq)
        case _ => {}
      }
    })
    ops
  }

  def useOp(t: String, sql: String, comment: Option[String]): Option[BaseOp] = {
    t match {
      case APP =>
        val name = sql.split("\\s+")(2).trim
        Option(new UseApp(if(name.endsWith(";")) name.substring(0, name.length-1) else name))
      case _ => None
    }
  }

  def createOp(t: String, sql: String, comment: Option[String]): Option[BaseOp] = {
    t match {
      case TABLE => Option(new CreateTable(sql))
      case EXTERNAL => Option(new CreateTable(sql, isExternal=true))
      case VIEW => Option(new CreateView(sql))
      case STREAM =>
        if(comment.isDefined && comment.get.contains(OP.NO_RESET)){
          Option(new CreateStream(sql, reset = false))
        }else{
          Option(new CreateStream(sql, reset = true))
        }
      case _ => None
    }
  }

  def isCreateSql(sql: String): Option[String] = {
    val tokens = toTokens(sql)
    if(tokens.length >= 2){
      val t1 = tokens(0)
      val t2 = tokens(1)
      if(t1 == OP.CREATE) {
        t2 match {
          case TABLE => Option(t2)
          case VIEW => Option(t2)
          case STREAM => Option(t2)
          case _ => None
        }
      }else{
        None
      }
    }else{
      None
    }
  }

  /**
    *
    * @param op
    * @param offset use it when table is external
    * @return
    */
  def getCreateOpId(op: String, offset: Int=0): String = {
    val index = op.toLowerCase().indexOf("create")
    val createSql = op.substring(index)
    val tokens = createSql.split("\\s+")

    val token = if(tokens(2).toLowerCase != "if"){
      tokens(2+offset)
    }else{
      tokens(5+offset) //skip "if not exists"
    }
    token.split("\\(")(0)
  }
}

object OP {
  val CREATE = "create"
  val USE = "use"
  val STREAM = "stream"
  val TARGET = "target"
  val ERROR = "error"
  val NO_RESET = "no-reset"
  val TIME_LIMIT = "time"
  val CMD = "cmd"
  val ANALYSE = "analyse"
}