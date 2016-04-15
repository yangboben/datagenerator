package io.transwarp.streaming.tester

import java.io.File

import scala.io.Source

/**
  * Created by tianming on 15-12-8.
  */
object SqlScriptParser {
  def parse(testcase: SqlTestCase, file: File): Array[(String, Option[String])] = {
    var inBlock = false
    var ops: Array[(String, Option[String])] = Array()

    val lines = Source.fromFile(file).getLines().toArray
    var segments: Array[String] = Array()
    lines.filter(_.length > 0).foreach(line => {
      //now only support '--' comment format
      val tokens = line.split("--").map(_.trim)
      val seg = tokens(0)
      val segL = seg.toLowerCase
      segments :+= seg
      val comment = if(tokens.length >=2) Option(tokens(1)) else None

      val sql: String = if(inBlock){
        if(segL.trim.startsWith(SQL.END)){
          // exit block
          inBlock = false
          val op = segments.mkString("\n").trim
          segments = Array()
          op
        }else{
          ""
        }
      } else {
        if(isBlock(segL)){
          inBlock = true
          ""
        }else{
          // simple sql statement
          if(segL.endsWith(";")) {
            val op = segments.mkString("\n").trim
            segments = Array()
            op
          }else{
            ""
          }
        }
      }

      comment.map(c => if(c.toLowerCase.startsWith("time")){
        val t = c.split(":")
        testcase.totalTimeLimit = t(1).toInt
      })
      ops :+= (sql, comment)
    })
    ops.filter(p => !(p._1.length == 0 && p._2.isEmpty))
  }
  def isBlock(seg: String): Boolean = {
    val segL = seg.toLowerCase
    val procedure = (segL.startsWith(SQL.CREATE) || segL.startsWith(SQL.REPLACE)) &&
                        (segL.contains(SQL.FUNCTION) || segL.contains(SQL.PROCEDURE) || segL.contains(SQL.PACKAGE))
    val block = segL.startsWith(SQL.BEGIN)
    val declare = segL.startsWith(SQL.DECLARE)
    procedure || block || declare
  }
}

object SQL {
  val CREATE = "create"
  val REPLACE = "replace"
  val FUNCTION = "function"
  val PROCEDURE = "procedure"
  val PACKAGE = "package"
  val DECLARE = "declare"
  val BEGIN = "begin"
  val END = "end;"
}