package io.transwarp.streaming.tester

import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.concurrent.TimeoutException

import io.transwarp.streaming.tester.SqlExecutor._
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Await, Future}
import scala.sys.process.{ProcessLogger, _}
import scala.util.{Failure, Success, Try}

/**
 * Created by tianming on 15-11-7.
 */
class SqlExecutor(host: String, database: String, principal: Option[String]=None, hive2: Boolean=true, useJdbc: Boolean=true) {
  var connection: Connection = _
  var stmt: Statement = _
  if(useJdbc) {
    connection = initConnection()
    stmt = connection.createStatement()
    if(principal.isDefined) {
      execQuery("set role admin;")
    }
  }

  def setTimeout(seconds: Int): Unit = {
    stmt.setQueryTimeout(seconds)
  }

  private def initConnection(): Connection= {
    val base = if(hive2) s"jdbc:hive2://${host}:10000/${database}"
    else s"jdbc:transwarp://${host}:10000/${database}"
    val connStr = principal match {
      case Some(p) =>
        val kuser = p
        val keytab = "/etc/inceptorsql2/hive.keytab"
        //val conf = new Configuration();
        //conf.set("hadoop.security.authentication", "kerberos");
        //UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(kuser, keytab)
        s"${base};principal=${p};authentication=kerberos"
      case None => base
    }
    if(hive2) {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
    }else{
      Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver")
    }

    var connection: Connection = null
    try{
      connection = DriverManager.getConnection(connStr, "", "")
    } catch {
      case e: SQLException => {
        val msg = s"Failed to connect to server $connStr"
        println(Console.RED + msg + Console.RESET)
        throw e
      }

    }
    connection
  }

  def closeConnection(): Unit = {
    if(useJdbc) {
      stmt.close()
      connection.close()
    }
  }

  def execQuery(query: String, timeout: Int= -1): Either[Array[String], Throwable] = {
    val queries = if(SqlScriptParser.isBlock(query)){
      Array(query)
    }else{
      query.split(";")
    }
    val res = queries.filter(_.length > 0).map(q => {
      //println(s"[debug]${q}[debug]")
      if(useJdbc) {
        execQueryJdbc(q, timeout)
      }else{
        execQueryCmd(q, timeout)
      }
    })
    if(res.length > 0) {
      res.last
    }else{
      Left(Array())
    }
  }

  private def execQueryCmd(query: String, timeout: Int): Either[Array[String], Throwable] = {
    var errors: Array[String] = Array()

    val lines = Seq("transwarp", "-t", "-h", host, "--database", database, "-e", query) lines_! ProcessLogger(line => errors = errors :+ line)
    if(errors.length > 0 || !errors.last.startsWith(TIME)) {
      Right(new Exception(errors.last))
    }else{
      Left(lines.toArray)
    }
  }

  private def execQueryJdbc(sql: String, timeout: Int): Either[Array[String], Throwable] = {
    Try{
      var rows: Array[String] = Array()
      val res = if(timeout > 0) {
        val f = Future ( stmt.execute(sql) )
        Await.result(f, Span(timeout, Seconds))
      } else{
        stmt.execute(sql)
      }
      //val res = stmt.execute(sql)
      if(res == true){
        var hasResultSet = true
        while(hasResultSet) {
          val resultSet = stmt.getResultSet()
          val meta = resultSet.getMetaData
          while(resultSet.next) {
            var row: Array[String] = Array()
            for(i <- 1 to meta.getColumnCount) {
              val v = resultSet.getString(i)
              if( v != null && v.trim.length > 0) {
                row :+= v
              }
            }
            rows :+= row.mkString(" ")
          }
          //hasResultSet = stmt.getMoreResults
          hasResultSet = false
        }
      }
      rows
    } match {
      case Success(rows) => Left(rows)
      case Failure(ex) =>
        ex match {
          case e: TimeoutException =>
            stmt.cancel()
          case e: Throwable => {
            //e.printStackTrace()
          }
        }
        //println(Console.RED + ex.getMessage + Console.RESET)
        Right(ex)
    }
  }
}

object SqlExecutor {
  val TIME = "Time"
}

