package org.apache.spark.performance

import java.io._
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Properties, Date}

import org.apache.spark.scheduler._
import org.apache.spark.util.JsonProtocol

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.json4s.jackson.JsonMethods._

import scala.xml.Node

/**
 * Created by liusheng on 12/19/15.
 */
object SqlReportGen {
  val appName = "inceptor"

  class JobInfo(val id: Int,
                val sql: String,
                val submitTime: Long,
                var finishTime: Long,
                var stageIds: ArrayBuffer[StageInfo],
                val properties: Properties)

  class JobInfo1(val jobInfo: JobInfo, var stageIds: ArrayBuffer[StageInfo2])

  class JobInfo2(val sql: String,
                 var jobInfoBase: Option[JobInfo1],
                 var jobInfoRegression: Option[JobInfo1],
                 var diffTime: Option[Long],
                 var diffTimeRate: Option[Double],
                 var stageDiff: ArrayBuffer[StageInfo3])

  class StageInfo2(var stageName: String,
                   var stageInfo: StageInfo,
                   var avg: Double)

  class StageInfo3(var stageName: String,
                   var stageInfoBase: StageInfo2,
                   var stageInfoReg: StageInfo2,
                   var diffTime: Option[Long],
                   var diffTimeRate: Option[Double])

  def getTimeFromDirName(dirName: String): String = {
    val splits = dirName.split("/")
    val splitsAgain = splits(splits.length - 2).split("-")
    splitsAgain(splitsAgain.length - 1)
  }

  def getDataMap(baseDir: String): mutable.HashMap[String, ArrayBuffer[JobInfo]] = {
    val resultMap = new mutable.HashMap[String, ArrayBuffer[JobInfo]]()
    val sqlJobIdMap = new mutable.HashMap[Int, String]()


    new File(baseDir).listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("EVENT_LOG")
    }).foreach(file => {
      for (line <- Source.fromFile(file).getLines) {
        try {
          JsonProtocol.sparkEventFromJson(parse(line)) match {
            case SparkListenerJobStart(jobId, time, stageIds, properties) =>
              val sql = properties.getProperty("spark.job.description")
              Some(sql).map(sql => {
                sqlJobIdMap.put(jobId, sql)
                resultMap
                  .getOrElseUpdate(sql, new mutable.ArrayBuffer[JobInfo]())
                  .append(new JobInfo(jobId, sql, time, 0, new ArrayBuffer[StageInfo](), properties))
              })

            case SparkListenerJobEnd(jobId, time, jobResult) =>
              jobResult match {
                case JobSucceeded => resultMap.get(sqlJobIdMap.get(jobId).getOrElse("")).map(_.filter(_.id == jobId).map(_.finishTime = time))
                case _ => resultMap.remove(sqlJobIdMap.get(jobId).getOrElse(""))
              }

            case SparkListenerStageSubmitted(stageInfo, properties) =>
            case SparkListenerStageCompleted(stageInfo) =>
              val sql = sqlJobIdMap.get(stageInfo.jobId).getOrElse("")
              resultMap.get(sql).map(_.filter(_.id == stageInfo.jobId).map(_.stageIds.append(stageInfo)))
            case SparkListenerTaskStart(stageId, stageAttemptId, taskInfo, taskMetrics) =>
            case SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo, taskMetrics) =>
          }
        } catch {
          case e: Exception =>
        }
      }
    })
    resultMap
  }

  def mergeStageInfo(allStages: ArrayBuffer[ArrayBuffer[StageInfo]]): ArrayBuffer[StageInfo2] = {
    val sortedAllStages = allStages.map(_.sortBy(_.stageId))
    val size = sortedAllStages.headOption.map(_.size).getOrElse(0)
    val result = new ArrayBuffer[StageInfo2]
    for (i <- 0 until size) {
      var sum = 0L
      for (j <- 0 until sortedAllStages.size) {
        sum += sortedAllStages(j)(i).completionTime.get - sortedAllStages(j)(i).submissionTime.get
      }
      val avg = sum.toDouble / sortedAllStages.size
      result append new StageInfo2(sortedAllStages.head(i).name, sortedAllStages.head(i), avg)
    }
    result
  }

  def mergeJobInfo(jobInfos: ArrayBuffer[JobInfo]): Option[JobInfo1] = {
    val avg = jobInfos.map(t => t.finishTime - t.submitTime).sum.toDouble / jobInfos.size
    val allStages = new ArrayBuffer[ArrayBuffer[StageInfo]]()
    jobInfos.foreach(i => allStages.append(i.stageIds))
    jobInfos.headOption.map(info =>
      new JobInfo1(info, mergeStageInfo(allStages)))
  }

  def makeMatch(baseData: mutable.HashMap[String, ArrayBuffer[JobInfo]],
                regressionData: mutable.HashMap[String, ArrayBuffer[JobInfo]]):
  mutable.HashMap[String, JobInfo2] = {
    val allSql = new mutable.HashSet[String]()
    allSql ++= baseData.keySet
    allSql ++= regressionData.keySet
    val resultMap = new mutable.HashMap[String, JobInfo2]()

    baseData.foreach(kv => {
      val sql = kv._1
      val jobInfo = mergeJobInfo(kv._2)
      jobInfo.map(job => resultMap.getOrElseUpdate(sql, new JobInfo2(sql, None, None, None, None, null))
        .jobInfoBase = jobInfo)
    })

    regressionData.foreach(kv => {
      val sql = kv._1
      val jobInfo = mergeJobInfo(kv._2)
      jobInfo.map(job => resultMap.getOrElseUpdate(sql, new JobInfo2(sql, None, None, None, None, null))
        .jobInfoRegression = jobInfo)
    })

    resultMap.foreach(x => {
      val jobInfo2: JobInfo2 = x._2
      if (jobInfo2.jobInfoBase.isDefined && jobInfo2.jobInfoRegression.isDefined) {
        val timeBase = jobInfo2.jobInfoBase.map(x => x.jobInfo.finishTime - x.jobInfo.submitTime)
        val timeRegression = jobInfo2.jobInfoRegression.map(x => x.jobInfo.finishTime - x.jobInfo.submitTime)
        jobInfo2.diffTime = timeRegression.map(_ - timeBase.get)
        jobInfo2.diffTimeRate = jobInfo2.diffTime.map(_.toDouble / timeRegression.get)
      }
      jobInfo2.stageDiff = makeStageMatch(jobInfo2.jobInfoBase.map(_.stageIds).getOrElse(new ArrayBuffer[StageInfo2]),
        jobInfo2.jobInfoRegression.map(_.stageIds).getOrElse(new ArrayBuffer[StageInfo2]))
    })
    resultMap
  }

  def max(a: Int, b: Int) = if (a > b) a else b

  def dim2i(rows: Int, cols: Int): Array[Array[Int]] = {
    val d2: Array[Array[Int]] = new Array(rows)
    for (k <- 0 until rows) {
      d2(k) = new Array[Int](cols)
    }
    d2
  }

  def makeStageMatch(baseStageInfo: ArrayBuffer[StageInfo2], regressionStageInfo: ArrayBuffer[StageInfo2]):
  ArrayBuffer[StageInfo3] = {
    val resultList = new ArrayBuffer[StageInfo3]
    for (i <- 0 until max(baseStageInfo.size, regressionStageInfo.size)) {
      resultList.append(new StageInfo3(baseStageInfo(i).stageName, baseStageInfo(i), regressionStageInfo(i),
        Some((regressionStageInfo(i).avg - baseStageInfo(i).avg).toLong), Some((regressionStageInfo(i).avg - baseStageInfo(i).avg) / regressionStageInfo(i).avg)))
    }
    resultList
  }

  def genTables(dataTable: mutable.HashMap[String, JobInfo2]): Seq[Node] = {
    val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val point2 = new DecimalFormat("#.00")
    dataTable.map(data => {
      val sql = data._1
      val jobInfo2 = data._2
      val jobColor = jobInfo2.diffTime.map(t => if (t >= 0) "color: red" else "color: green").getOrElse("")
      <h4>{sql}</h4>
      <table class="table table-bordered table-striped table-condensed">
        <thead>
          <tr>
          <th></th>
          <th>Baseline</th>
          <th>Regression</th>
          <th>Diff</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Total Time</td>
            <td>{jobInfo2.jobInfoBase.map(t => {s"${point2.format(((t.jobInfo.finishTime - t.jobInfo.submitTime) / 1000.0))}s"}).getOrElse("")}</td>
            <td>{jobInfo2.jobInfoRegression.map(t => {s"${point2.format(((t.jobInfo.finishTime - t.jobInfo.submitTime) / 1000.0))}s"}).getOrElse("")}</td>
            <td>{jobInfo2.jobInfoRegression.map(r => jobInfo2.jobInfoBase.map(b => {

              val color = if (jobInfo2.diffTime.get >= 0) "color: red" else "color: green"
              <div style={color}>{s"${jobInfo2.diffTime.get / 1000.0}s (${jobInfo2.diffTimeRate.get * 100}%)"}</div>
            }).getOrElse(Nil)).getOrElse(Nil)}</td>
          </tr>
          {jobInfo2.stageDiff.map(diff => {
            <tr>
              <td>{diff.stageName}</td>
              <td>{point2.format((diff.stageInfoBase.stageInfo.completionTime.get - diff.stageInfoBase.stageInfo.submissionTime.get) / 1000.0)}</td>
              <td>{point2.format((diff.stageInfoReg.stageInfo.completionTime.get - diff.stageInfoReg.stageInfo.submissionTime.get) / 1000.0)}</td>
              <td>{diff.diffTime.map(t => {

                val color = if (t >= 0) "color: red" else "color: green"
                <div style={color}>{s"${t / 1000.0}s (${point2.format(diff.diffTimeRate.get * 100)}%)"}</div>
              }).getOrElse(Nil)}</td>
            </tr>
          }).foldLeft[Seq[Node]](Nil)(_ ++ _)}
        </tbody>
      </table>
    }).foldLeft[Seq[Node]](Nil)(_ ++ _)
  }

  def main(args: Array[String]): Unit = {
    val baseDir = args(0)
    val regressionDir = args(1)
    val outputFile = args(2)

    val baseData = getDataMap(baseDir)
    val regressionData = getDataMap(regressionDir)
    val diffTable = makeMatch(baseData, regressionData)

      val template =
        <html>
          <head>
            <meta http-equiv="Content-type" content="text/html; charset=utf-8"/>
            <link rel="stylesheet" href="static/bootstrap.min.css" type="text/css"/>
            <link rel="stylesheet" href="static/vis.min.css" type="text/css"/>
            <link rel="stylesheet" href="static/webui.css" type="text/css"/>
            <link rel="stylesheet" href="static/timeline-view.css" type="text/css"/>

            <script src="static/sorttable.js"></script>
            <script src="static/jquery-1.11.1.min.js"></script>
            <script src="static/vis.min.js"></script>
            <script src="static/bootstrap-tooltip.js"></script>
            <script src="static/initialize-tooltips.js"></script>
            <script src="static/table.js"></script>
            <script src="static/additional-metrics.js"></script>
            <script src="static/timeline-view.js"></script>
            <title>Inceptor statistics report</title>
          </head>
          <body>
            <div class="navbar navbar-static-top">
              <div class="navbar-inner">
                <a href="/" class="brand">
                  <img src="static/spark-logo-77x50px-hd.png"/>
                </a>
                <ul class="nav">
                  <li class="active">
                    <a href="/job">Job</a>
                  </li> <li class=" ">
                  <a href="/stage">Stage</a>
                </li>
                </ul>
                <p class="navbar-text pull-right">
                  <strong>Inceptor statistics report</strong>
                </p>
              </div>
            </div>
            <div class="container-fluid">
              <div class="row-fluid">
                {genTables(diffTable)}
              </div>
            </div>
          </body>
        </html>
      val pw = new PrintWriter(outputFile)
      pw.print(template.toString)
      pw.close()
  }
}
