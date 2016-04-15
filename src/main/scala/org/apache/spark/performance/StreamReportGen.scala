package org.apache.spark.performance

import java.io._
import java.text.{SimpleDateFormat, NumberFormat}
import java.util
import java.util.Properties

import org.apache.spark.scheduler._
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.Node

/**
 * Created by liusheng on 12/19/15.
 */

case class JobInfo(id: Int,
              submitTime: Long,
              var finishTime: Long,
              var totalTime: Long,
              stageInfos: ArrayBuffer[StageInfo],
              properties: Properties)

case class JobInfoAvg(
                   var totalTime: Long,
                   stageInfos: ArrayBuffer[StageInfoAvg])

case class StageInfoAvg(stageName: String, avgTime: Long)

case class StreamInfo(sql: String, startTime: Long, streamId: String, totalTime: Long, jobInfos: ArrayBuffer[JobInfo])

case class StreamInfoAvg(sql: String, startTime: Long, streamId: String, totalTime: Long, batchCount: Long, jobInfo: JobInfoAvg)

object StreamReportGen {

  def getJobInfos(subDir: File): Seq[StreamInfo] = {
    val streamIdToJobInfos = new mutable.HashMap[String, mutable.HashMap[Long, JobInfo]]
    val sqlJobIdMap = new mutable.HashMap[Int, String]()

    subDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("inceptor")
    }).flatMap(_.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("EVENT")
    })).foreach(file => {
      println(file.getPath)
      for (line <- Source.fromFile(file).getLines) {
        try {
          JsonProtocol.sparkEventFromJson(parse(line)) match {
            case SparkListenerJobStart(jobId, time, stageIds, properties) =>
              val streamId = Some(properties.getProperty("jobOwner"))
              streamId.map( id => {
                if (!id.equals("null")) {
                  sqlJobIdMap.put(jobId, id)
                  streamIdToJobInfos.getOrElseUpdate(id, new mutable.HashMap[Long, JobInfo])
                    .put(jobId, JobInfo(jobId, time, 0, 0, new ArrayBuffer[StageInfo], properties))
                }})
            case SparkListenerJobEnd(jobId, time, jobResult) =>
              val streamId = sqlJobIdMap.get(jobId)
              streamId.map(id => {
                val jobInfo = streamIdToJobInfos.get(id).map(infos => infos.get(jobId)).get.get
                jobResult match {
                  case JobSucceeded =>
                    jobInfo.finishTime = time
                    jobInfo.totalTime = jobInfo.finishTime - jobInfo.submitTime
                  case _ =>
                    streamIdToJobInfos.get(id).map(_.remove(jobId))
                    sqlJobIdMap.remove(jobId)
                }
              })
            case SparkListenerStageSubmitted(stageInfo, properties) =>
            case SparkListenerStageCompleted(stageInfo) =>
              val streamId = sqlJobIdMap.get(stageInfo.jobId)
              streamId.map(id => {
                streamIdToJobInfos.get(id).map(info => {
                  info.get(stageInfo.jobId).map(_.stageInfos.append(stageInfo))
                })
              })
            case SparkListenerTaskStart(stageId, stageAttemptId, taskInfo, taskMetrics) =>
            case SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo, taskMetrics) =>
          }
        } catch {
          case e: Exception =>
        }
      }
    })
    val res = streamIdToJobInfos.map(kv => {
      val minSubmitTime = kv._2.values.minBy(_.submitTime).submitTime
      val maxFinishTime = kv._2.values.maxBy(_.finishTime).finishTime
      val stages = new ArrayBuffer[JobInfo]()
      kv._2.values.foreach(x => stages.append(x))
      StreamInfo("", 0, kv._1, maxFinishTime - minSubmitTime, stages)
    }).toSeq
    res
  }

  def mkAverage(streamInfo: StreamInfo): StreamInfoAvg = {
    val totalTimeAvg = streamInfo.jobInfos.foldLeft(0L)(_ + _.totalTime) / streamInfo.jobInfos.size
    val stageInfosize = streamInfo.jobInfos.head.stageInfos.size
    val mergedStageInfos = new ArrayBuffer[ArrayBuffer[StageInfo]]()
    (0 until stageInfosize).foreach(x => mergedStageInfos.append(new ArrayBuffer[StageInfo]))
    val stageInfos = streamInfo.jobInfos
      .map(_.stageInfos.sortBy(_.stageId))
      .foreach(stages => for(i <- 0 until  stages.size){
        mergedStageInfos(i).append(stages(i))
      })
    val stageAvg = mergedStageInfos.map(stageInfos => {
      val avg = stageInfos.foldLeft(0L)((z, x) => z + x.completionTime.get - x.submissionTime.get) / stageInfos.size
      StageInfoAvg(stageInfos.head.name, avg)
    })
    StreamInfoAvg(streamInfo.sql, streamInfo.startTime, streamInfo.streamId, streamInfo.totalTime,
      streamInfo.jobInfos.size, JobInfoAvg(totalTimeAvg, stageAvg))
  }

  def makeDiffStages(stageInfoBase: ArrayBuffer[StageInfoAvg], stageInfosReg: ArrayBuffer[StageInfoAvg]): ArrayBuffer[StageInfoAvg] = {
    val result = new ArrayBuffer[StageInfoAvg]()
    stageInfoBase.zip(stageInfosReg).map(t => StageInfoAvg(t._1.stageName, t._2.avgTime - t._1.avgTime) )
  }

  def makeDiff(base: StreamInfoAvg, reg: StreamInfoAvg): StreamInfoAvg = {
    val jobInfoAvg = JobInfoAvg(reg.jobInfo.totalTime - base.jobInfo.totalTime,
                                makeDiffStages(base.jobInfo.stageInfos, reg.jobInfo.stageInfos))
    StreamInfoAvg(base.sql,
      base.startTime,
      base.streamId,
      reg.totalTime - base.totalTime,
      reg.batchCount - base.batchCount,
      jobInfoAvg)
  }

  def genTables(streamInfoBase: Map[String, StreamInfoAvg], streamInfoReg: Map[String, StreamInfoAvg], diff: mutable.HashMap[String, StreamInfoAvg]): Seq[Node] = {
    val ddf1 = NumberFormat.getNumberInstance
    ddf1.setMaximumFractionDigits(2)
    streamInfoBase.map(base => {
    val sql = base._1
    val streamInfo = base._2
      <h4>{streamInfo.streamId}</h4>
      <h4>{streamInfo.sql}</h4>
      <table class="table table-bordered table-striped table-condensed">
        <thead>
          <th></th>
          <th>baseline</th>
          <th>regression</th>
          <th>diff</th>
        </thead>
        <tbody>
          <tr>
            <td>total time</td>
            <td>{streamInfo.totalTime / 1000.0 + "s"}</td>
            <td>{streamInfoReg.get(sql).map(_.totalTime / 1000.0 + "s").getOrElse("")}</td>

            <td>{streamInfoReg.get(sql).map(t => {
                                            val color = if (diff(sql).totalTime > 0) "color: red" else "color: green"
                                            <div style={color}>{(diff(sql).totalTime) / 1000.0 + "s (" + ddf1.format((diff(sql).totalTime.toDouble / streamInfo.totalTime) * 100) + "%)"}</div>})
                                            .getOrElse(Nil)}</td>
          </tr>

          <tr>
            <td>batch count</td>
            <td>{streamInfo.batchCount}</td>
            <td>{streamInfoReg.get(sql).map(_.batchCount).getOrElse("")}</td>

            <td>{streamInfoReg.get(sql).map(t => {
                                            val color = if (diff(sql).batchCount > 0) "color: red" else "color: green"
                                            <div style={color}>{(diff(sql).batchCount)}</div>})
                                            .getOrElse(Nil)}</td>
          </tr>

          <tr>
            <td>time per batch</td>
            <td>{streamInfo.jobInfo.totalTime / 1000.0 + "s"}</td>
            <td>{streamInfoReg.get(sql).map(_.jobInfo.totalTime / 1000.0 + "s").getOrElse("")}</td>

            <td>{streamInfoReg.get(sql).map(t => {
                                            val color = if (diff(sql).jobInfo.totalTime > 0) "color: red" else "color: green"
                                            <div style={color}>{(diff(sql).jobInfo.totalTime) / 1000.0 + "s (" + ddf1.format((diff(sql).jobInfo.totalTime.toDouble / streamInfo.jobInfo.totalTime) * 100) + "%)"}</div>})
                                            .getOrElse(Nil)}</td>
          </tr>

          {streamInfo.jobInfo.stageInfos.zipWithIndex.map(si => {
              val info = si._1
              val index = si._2
              <tr>
                <td>{info.stageName}</td>
                <td>{info.avgTime / 1000.0 + "s"}</td>
                <td>{streamInfoReg.get(sql).map(_.jobInfo.stageInfos(index).avgTime / 1000.0 + "s").getOrElse("")}</td>
                <td>{diff.get(sql).map(t => {
                  val time = t.jobInfo.stageInfos(index).avgTime
                  val color = if (time > 0) "color: red" else "color: green"
                  <div style={color}>{time / 1000.0 + "s (" + ddf1.format((time.toDouble / info.avgTime) * 100) + "%)"}</div>
                  }).getOrElse(Nil)}</td>
              </tr>
          }).foldLeft[Seq[Node]](Nil)(_ ++ _)}

        </tbody>
        <tfoot>
        </tfoot>
      </table>
    }).foldLeft[Seq[Node]](Nil)(_ ++ _)
  }

  def main(args: Array[String]): Unit = {
    val baseDir = args(0)
    val regDir = args(1)
    val outputFile = args(2)

    val streamInfoBase = getJobInfos(new File(baseDir)).map(mkAverage(_)).map(t => (t.streamId,t)).toMap
    val streamInfoReg = getJobInfos(new File(regDir)).map(mkAverage(_)).map(t => (t.streamId,t)).toMap
    val baseAndReg = new mutable.HashMap[String, (StreamInfoAvg, StreamInfoAvg)]()
    streamInfoBase.keySet.foreach(sql => {streamInfoReg.get(sql).map(reg => {
      baseAndReg.put(sql, (streamInfoBase.get(sql).get, reg))
    })})
    val result: mutable.HashMap[String, StreamInfoAvg] = baseAndReg.map(kv => (kv._1, makeDiff(kv._2._1, kv._2._2)))
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
                  <strong>{new SimpleDateFormat("yyyy-MM-dd HH:mm:ss E").format(new java.util.Date())}</strong>
                </p>
              </div>
            </div>
            <div class="container-fluid">
              <div class="row-fluid">
                {genTables(streamInfoBase, streamInfoReg, result)}
              </div>
            </div>
          </body>
        </html>
      val pw = new PrintWriter(outputFile)
      pw.print(template.toString)
      pw.close()
  }
}
