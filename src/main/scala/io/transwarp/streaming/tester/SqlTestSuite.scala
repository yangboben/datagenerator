package io.transwarp.streaming.tester

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FilenameUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FunSuite, ParallelTestExecution, Suite}

/**
  * Created by tianming on 15-12-7.
  */
class SqlTestSuite(tester: StreamSQLTester, val suite: File, include: Array[String] = null, report: String) extends FunSuite with Eventually{
  override def suiteName:String = tester.testsRoot.relativize(suite.toPath).toString.replace("/", "_")
  override def suiteId: String = suiteName
  runSuite(suite, include, report)

  def runSuite(suite: File, include: Array[String] = null, report: String): Unit = {
    val p = suite.toPath
    val scriptsRoot = p.resolve("sql")
    if (Files.exists(scriptsRoot)) {
      var scripts = scriptsRoot.toFile.listFiles.filter(_.getName.endsWith("sql"))
      if (include != null && !include.isEmpty) {
        scripts = scripts.filter(f => include.contains(FilenameUtils.getBaseName(f.getName)))
      }
      scripts.foreach(file => {
        test(s"${file.getName}") {
          println(s"------- ${suiteName}/${file.getName} ----------")

          val basename = FilenameUtils.getBaseName(file.getName)
          val testcase = new SqlTestCase(this, tester, basename, if (report != null) Some(this.info) else None)
          runCase(testcase, tester.clean)
        }
      })
    }
  }

  def runCase(testcase: SqlTestCase, clean: Boolean): Unit ={
    println("-- init --")
    testcase.before()
    try {
      println("-- start --")
      eventually(timeout(Span(testcase.totalTimeLimit, Seconds)), interval(Span(2, Seconds))) {
        val error = testcase.run()
        assert(error.isEmpty == true, error.getOrElse(""))
      }
    } finally {
      if (clean) {
        println("-- clean --")
        testcase.after()
      }
      if (!testcase.isPassed) {
        testcase.toFile(tester.testsRoot.resolve(s"failed/${testcase.name}.sql").toString)
      }
    }
  }
}

class SqlTestSuiteParallel(tester: StreamSQLTester, suite: File, include: Array[String] = null, report: String) extends
SqlTestSuite(tester, suite, include, report) with ParallelTestExecution{
  override def newInstance: Suite with ParallelTestExecution = {
    val instance = new SqlTestSuiteParallel(tester, suite, include, report)
    instance
  }
}