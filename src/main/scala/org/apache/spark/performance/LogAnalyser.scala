package org.apache.spark.performance

import java.io.{FileFilter, PrintWriter, File}

import io.transwarp.streaming.tester.SqlTestCase
import org.apache.spark.scheduler._
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods._

import scala.io.Source
import scala.sys.process._
import scala.sys.process.ProcessLogger

/**
 * Created by liusheng on 11/14/15.
 */
class LogAnalyser(testcase: SqlTestCase) {

  class AnalyseInfo(val batchCount: Long, val totalTime: Double) {}

  def copyReportFiles(src: String, dest: String): Unit ={
    val command = Seq("scp", "-r", src, dest)
    println(command)
    command lines_! ProcessLogger(line => line)
  }

  def cleanReportFiles(dest: String): Unit = {
    Seq("rm", "-rf", dest) lines_! ProcessLogger(line => line)
  }

  def getLatestReportPath(baseDir: String, streamId: String): Seq[String] = {
    val dirFile = new File(baseDir)
    val subdirs = dirFile.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.startsWith(streamId)
    })
    subdirs.sortBy(_.toString).takeRight(1).map(_.toString)
  }

  def analyseOneReport(logFile: String) = {
    var batchCount = 0L
    var beginTime = 0L
    var endTime = 0L
    println(s"${logFile} + /EVENT_LOG_1")
    val lines = Source.fromFile(logFile + "/EVENT_LOG_1").getLines().foreach(tmp => {
      try {
        JsonProtocol.sparkEventFromJson(parse(tmp)) match {
          case SparkListenerJobStart(jobId, time, stageIds, properties) =>
          case SparkListenerJobEnd(jobId, time, jobResult) => batchCount += 1
          case SparkListenerStageSubmitted(stageInfo, properties) =>
          case SparkListenerStageCompleted(stageInfo) =>
            if (beginTime == 0) {beginTime = stageInfo.submissionTime.get}
            endTime = stageInfo.completionTime.get
          case SparkListenerTaskStart(stageId, stageAttemptId, taskInfo, taskMetrics) =>
          case SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo, taskMetrics) =>
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
    val resultThis = new AnalyseInfo(batchCount, (endTime - beginTime)/1000.0)
    testcase.printMessage(s"Last Run   batchCount: ${resultThis.batchCount}, totalTime: ${resultThis.totalTime}")
  }
}
