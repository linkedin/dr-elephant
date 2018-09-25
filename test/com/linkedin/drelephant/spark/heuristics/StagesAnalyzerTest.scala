package com.linkedin.drelephant.spark.heuristics

import java.util.Date

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters

/** Tests for the StagesAnalyzer. */
class StagesAnalyzerTest extends FunSpec with Matchers {
  import SparkTestUtilities._

  describe("StagesAnalyzer") {
    it("has task failures severity") {
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).create(),
        StageBuilder(2, 5).failures(2, 2, 0).create(),
        StageBuilder(3, 15).failures(2, 0, 1).create(),
        StageBuilder(4, 15).failures(3, 1, 2).create(),
        StageBuilder(5, 4).failures(2, 0, 0).status(StageStatus.FAILED, Some("array issues")).create())
      val properties = Map( "spark.sql.shuffle.partitions" -> "200")
      val data = createSparkApplicationData(stages, Seq.empty, Some(properties))

      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 3).create(),
        StageAnalysisBuilder(2, 5)
          .taskFailures(Severity.CRITICAL, Severity.CRITICAL, Severity.NONE, 8, 2, 2, 0,
            Seq("Stage 2 has 2 failed tasks.",
              "Stage 2: has 2 tasks that failed because of OutOfMemory exception."))
          .create(),
        StageAnalysisBuilder(3, 15)
          .taskFailures(Severity.MODERATE, Severity.NONE, Severity.LOW, 4, 2, 0, 1,
            Seq("Stage 3 has 2 failed tasks.",
              "Stage 3: has 1 tasks that failed because the container was killed by YARN for exeeding memory limits."))
          .create(),
        StageAnalysisBuilder(4, 15)
          .taskFailures(Severity.CRITICAL, Severity.LOW, Severity.MODERATE, 12, 3, 1, 2,
            Seq("Stage 4 has 3 failed tasks.",
              "Stage 4: has 1 tasks that failed because of OutOfMemory exception.",
              "Stage 4: has 2 tasks that failed because the container was killed by YARN for exeeding memory limits."))
          .create(),
        StageAnalysisBuilder(5, 4)
          .taskFailures(Severity.CRITICAL, Severity.NONE, Severity.NONE, 8, 2, 0, 0,
            Seq("Stage 5 has 2 failed tasks."))
            .stageFailure(Severity.CRITICAL, 16, Seq("Stage 5 failed: array issues"))
          .create())

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis()
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
    }

    it("has task skew severity") {
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 5).taskRuntime(200, 250, 600).create(),
        StageBuilder(2, 5).taskRuntime(100, 250, 260).input(5, 250, 260).create(),
        StageBuilder(3, 5).taskRuntime(20, 250, 53).create(),
        StageBuilder(4, 5).taskRuntime(5, 250, 260).input(5, 250, 260).create(),
        StageBuilder(5, 5).taskRuntime(50, 250, 350).shuffleRead(50, 250, 350).shuffleWrite(50, 250, 400).create(),
        StageBuilder(6, 5).taskRuntime(50, 250, 350).shuffleRead(50, 50, 50).output(50, 50, 50).create(),
        StageBuilder(7, 5).taskRuntime(20, 250, 290).shuffleWrite(250, 250, 600).output(20, 250, 290).create(),
        StageBuilder(8, 3).taskRuntime(200, 250, 1000).create(),
        StageBuilder(9, 3).taskRuntime(5, 250, 70).create(),
        StageBuilder(10, 3).taskRuntime(20, 250, 300).input(20, 250, 300).create(),
        StageBuilder(11, 3).taskRuntime(50, 250, 350).shuffleRead(50, 250, 350).create(),
        StageBuilder(12, 5).taskRuntime(2, 50, 53).times("09/09/2018 12:00:00", "09/09/2018 12:01:00").create(),
        StageBuilder(13, 5).taskRuntime(5, 50, 60).input(50, 500, 600).create(),
        StageBuilder(14, 5).taskRuntime(5, 200, 210).output(5, 200, 210).create())
      val properties = Map( "spark.sql.shuffle.partitions" -> "5")
      val data = createSparkApplicationData(stages, Seq.empty, Some(properties))

      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 5).taskRuntime(200, 250)
            .longTask(Severity.LOW, 0, Seq()).create(),
        StageAnalysisBuilder(2, 5).taskRuntime(100, 250).input(260)
          .skew(Severity.LOW, Severity.LOW, 0,
            Seq()).create(),
        StageAnalysisBuilder(3, 5).taskRuntime(20, 250)
          .skew(Severity.SEVERE, Severity.SEVERE, 15,
            Seq("Stage 3 has skew in task run time (median is 20.00 sec, max is 4.17 min)",
              "Stage 3: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(4, 5).taskRuntime(5, 250).input(260)
          .skew(Severity.CRITICAL, Severity.CRITICAL, 20,
            Seq("Stage 4 has skew in task run time (median is 5.00 sec, max is 4.17 min)",
              "Stage 4 has skew in task input bytes (median is 5 MB, max is 250 MB).",
              "Stage 4: please set DaliSpark.SPLIT_SIZE to make partitions more even.")).create(),
        StageAnalysisBuilder(5, 5).taskRuntime(50, 250)
          .skew(Severity.MODERATE, Severity.MODERATE, 10,
            Seq("Stage 5 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 5 has skew in task shuffle read bytes (median is 50 MB, max is 250 MB).",
              "Stage 5 has skew in task shuffle write bytes (median is 50 MB, max is 250 MB).",
              "Stage 5: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(6, 5).taskRuntime(50, 250)
          .skew(Severity.MODERATE, Severity.MODERATE, 10,
            Seq( "Stage 6 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 6: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(7, 5).taskRuntime(20, 250)
          .skew(Severity.SEVERE, Severity.SEVERE, 15,
        Seq("Stage 7 has skew in task run time (median is 20.00 sec, max is 4.17 min)",
          "Stage 7 has skew in task output bytes (median is 20 MB, max is 250 MB).",
          "Stage 7: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(8, 3).taskRuntime(200, 250)
            .longTask(Severity.LOW, 0, Seq()).create(),
        StageAnalysisBuilder(9, 3).taskRuntime(5, 250)
          .skew(Severity.CRITICAL, Severity.CRITICAL, 12,
            Seq("Stage 9 has skew in task run time (median is 5.00 sec, max is 4.17 min)",
              "Stage 9: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(10, 3).taskRuntime(20, 250).input(300)
          .skew(Severity.SEVERE, Severity.SEVERE, 9,
            Seq("Stage 10 has skew in task run time (median is 20.00 sec, max is 4.17 min)",
              "Stage 10 has skew in task input bytes (median is 20 MB, max is 250 MB).",
              "Stage 10: please set DaliSpark.SPLIT_SIZE to make partitions more even.")).create(),
        StageAnalysisBuilder(11, 3).taskRuntime(50, 250)
          .skew(Severity.MODERATE, Severity.MODERATE, 6,
            Seq("Stage 11 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 11 has skew in task shuffle read bytes (median is 50 MB, max is 250 MB).",
              "Stage 11: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(12, 5).taskRuntime(2, 50).duration(60)
          .skew(Severity.CRITICAL, Severity.NONE, 0,
            Seq()).create(),
        StageAnalysisBuilder(13, 5).taskRuntime(5, 50).input(600)
          .skew(Severity.SEVERE, Severity.NONE, 0,
            Seq()).create(),
        StageAnalysisBuilder(14, 5).taskRuntime(5, 200)
          .skew(Severity.CRITICAL, Severity.NONE, 0,
            Seq()).create())

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis()
       (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
    }

    it("has long task severity") {
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(120, 150, 300).create(),
        StageBuilder(2, 3).taskRuntime(180, 200, 400).create(),
        StageBuilder(3, 3).taskRuntime(400, 500, 1000).create(),
        StageBuilder(4, 3).taskRuntime(700, 900, 2000).create(),
        StageBuilder(5, 3).taskRuntime(1200, 1500, 4000).create(),
        StageBuilder(6, 3).taskRuntime(700, 3500, 4500).create(),
        StageBuilder(7, 2).taskRuntime(700, 900, 2000).create(),
        StageBuilder(8, 3).taskRuntime(3000, 3000, 9000).input(2 << 20, 3 << 20, 5 << 20).create())
      val properties = Map( "spark.sql.shuffle.partitions" -> "3")
      val data = createSparkApplicationData(stages, Seq.empty, Some(properties))

      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 3).taskRuntime(120, 150).create(),
        StageAnalysisBuilder(2, 3).taskRuntime(180, 200).longTask(Severity.LOW, 0, Seq()).create(),
        StageAnalysisBuilder(3, 3).taskRuntime(400, 500).longTask(Severity.MODERATE, 6,
          Seq("Stage 3 median task run time is 6.67 min.")).create(),
        StageAnalysisBuilder(4, 3).taskRuntime(700, 900).longTask(Severity.SEVERE, 9,
          Seq("Stage 4 median task run time is 11.67 min.")).create(),
        StageAnalysisBuilder(5, 3).taskRuntime(1200, 1500).longTask(Severity.CRITICAL, 12,
          Seq("Stage 5 median task run time is 20.00 min.")).create(),
        StageAnalysisBuilder(6, 3).taskRuntime(700, 3500).longTask(Severity.SEVERE, 9,
          Seq("Stage 6 median task run time is 11.67 min."))
          .skew(Severity.MODERATE, Severity.MODERATE, 6,
            Seq("Stage 6 has skew in task run time (median is 11.67 min, max is 58.33 min)",
            "Stage 6: please try to modify the application to make the partitions more even.")).create(),
        StageAnalysisBuilder(7, 2).taskRuntime(700, 900).longTask(Severity.SEVERE, 6,
          Seq("Stage 7 median task run time is 11.67 min.",
          "Stage 7: please increase the number of partitions, which is currently set to 2.")).create(),
        StageAnalysisBuilder(8, 3).taskRuntime(3000, 3000).longTask(Severity.CRITICAL, 12,
          Seq("Stage 8 median task run time is 50.00 min.",
            "Stage 8: please set DaliSpark.SPLIT_SIZE to a smaller value to increase the number of tasks reading input data for this stage."))
          .input(5 << 20).create())

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis()
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
     }

    it("has execution memory spill severity") {
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 5).taskRuntime(100, 150, 400).shuffleRead(200, 300, 800)
          .spill(1, 2, 5).create(),
        StageBuilder(2, 5).taskRuntime(100, 150, 400).shuffleRead(200, 300, 800)
          .spill(10, 15, 40).create(),
        StageBuilder(3, 5).taskRuntime(100, 150, 400).input(500, 2000, 3000)
          .spill(100, 150, 400).create(),
        StageBuilder(4, 5).taskRuntime(300, 350, 1500).shuffleWrite(1000, 1000,5000)
          .spill(300, 350, 1500).create(),
        StageBuilder(5, 5).taskRuntime(300, 2500, 3000).shuffleRead(1000, 5000,16000)
          .shuffleWrite(300, 2500, 3000).spill(300, 2500, 3000).create(),
        StageBuilder(6, 3).taskRuntime(50, 250, 350).input(50, 250, 350)
          .spill(250, 250, 750).create(),
        StageBuilder(7, 3).taskRuntime(50, 250, 350).output(250, 1000, 1500)
          .spill(250, 250, 750).create(),
        StageBuilder(8, 5).taskRuntime(2, 50, 53)
          .times("09/09/2018 12:00:00", "09/09/2018 12:01:00")
            .shuffleRead(500, 500, 1500).spill(250, 250, 750).create(),
        StageBuilder(9, 5).taskRuntime(50, 250, 350).output(50, 250, 6 << 20)
          .spill(50, 250, 2L << 20).create(),
        StageBuilder(10, 5).taskRuntime(50, 250, 350).input(50, 250, 6 << 20)
          .spill(50, 250, 2L << 20).create(),
        StageBuilder(11, 3).taskRuntime(50, 250, 350).input(50, 250, 6 << 20)
          .spill(50, 250, 3L << 20).create(),
        StageBuilder(12, 3).taskRuntime(50, 250, 350).output(50, 250, 6 << 20)
          .spill(50, 250, 4L << 20).create())
      val properties = Map( "spark.sql.shuffle.partitions" -> "5")
      val data = createSparkApplicationData(stages, Seq.empty, Some(properties))

      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 5).taskRuntime(100, 150)
          .spill(Severity.NONE, Severity.NONE, 0, 2, 5, Seq()).create(),
        StageAnalysisBuilder(2, 5).taskRuntime(100, 150)
          .spill(Severity.LOW, Severity.LOW, 0, 15, 40, Seq()).create(),
        StageAnalysisBuilder(3, 5).taskRuntime(100, 150).input(3000)
          .spill(Severity.MODERATE, Severity.MODERATE, 10, 150, 400,
            Seq("Stage 3 has 400 MB execution memory spill."))
          .skew(Severity.NONE, Severity.NONE, 0,
            Seq("Stage 3 has skew in task input bytes (median is 500 MB, max is 1.95 GB).",
              "Stage 3: please set DaliSpark.SPLIT_SIZE to make partitions more even."))
          .create(),
        StageAnalysisBuilder(4, 5).taskRuntime(300, 350)
          .longTask(Severity.MODERATE, 10,
            Seq("Stage 4 median task run time is 5.00 min."))
          .spill(Severity.SEVERE, Severity.SEVERE, 15, 350, 1500,
            Seq("Stage 4 has 1.46 GB execution memory spill.")).create(),
        StageAnalysisBuilder(5, 5).taskRuntime(300, 2500)
          .longTask(Severity.MODERATE, 10, Seq("Stage 5 median task run time is 5.00 min."))
          .skew(Severity.SEVERE, Severity.SEVERE, 15,
            Seq("Stage 5 has skew in task run time (median is 5.00 min, max is 41.67 min)",
            "Stage 5 has skew in memory bytes spilled (median is 300 MB, max is 2.44 GB).",
            "Stage 5 has skew in task shuffle read bytes (median is 1,000 MB, max is 4.88 GB).",
            "Stage 5 has skew in task shuffle write bytes (median is 300 MB, max is 2.44 GB).",
            "Stage 5: please try to modify the application to make the partitions more even."))
          .spill(Severity.MODERATE, Severity.MODERATE, 10, 2500, 3000
            , Seq("Stage 5 has 2.93 GB execution memory spill.")).create(),
        StageAnalysisBuilder(6, 3).taskRuntime(50, 250).input(350)
          .skew(Severity.MODERATE, Severity.MODERATE, 6,
            Seq("Stage 6 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 6 has skew in task input bytes (median is 50 MB, max is 250 MB).",
              "Stage 6: please set DaliSpark.SPLIT_SIZE to make partitions more even."))
          .spill(Severity.CRITICAL, Severity.CRITICAL, 12, 250, 750,
            Seq("Stage 6 has 750 MB execution memory spill.")).create(),
        StageAnalysisBuilder(7, 3).taskRuntime(50, 250)
          .skew(Severity.MODERATE, Severity.MODERATE, 6,
            Seq("Stage 7 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
            "Stage 7 has skew in task output bytes (median is 250 MB, max is 1,000 MB).",
            "Stage 7: please try to modify the application to make the partitions more even."))
          .spill(Severity.CRITICAL, Severity.CRITICAL, 12, 250, 750,
            Seq("Stage 7 has 750 MB execution memory spill.")).create(),
        StageAnalysisBuilder(8, 5).taskRuntime(2, 50).duration(60)
          .skew(Severity.CRITICAL, Severity.NONE, 0,
            Seq("Stage 8: please try to modify the application to make the partitions more even."))
          .spill(Severity.CRITICAL, Severity.CRITICAL, 20, 250, 750,
            Seq("Stage 8 has 750 MB execution memory spill.")).create(),
        StageAnalysisBuilder(9, 5).taskRuntime(50, 250)
          .skew(Severity.MODERATE, Severity.MODERATE, 10,
            Seq("Stage 9 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 9 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
              "Stage 9 has skew in task output bytes (median is 50 MB, max is 250 MB).",
              "Stage 9: please try to modify the application to make the partitions more even."))
          .spill(Severity.SEVERE, Severity.NONE, 0, 250, 2L << 20,
            Seq("Stage 9 is processing a lot of data; examine the application to see if this can be reduced.",
              "Stage 9 has 2 TB execution memory spill.",
              "Stage 9 has 5 tasks, 0 B input read, 0 B shuffle read, 0 B shuffle write, 6 TB output.",
              "Stage 9 has median task values: 50 MB memory spill, 0 B input, 0 B shuffle read, 0 B shuffle write, 50 MB output."))
          .create(),
        StageAnalysisBuilder(10, 5).taskRuntime(50, 250).input(6 << 20)
          .skew(Severity.MODERATE, Severity.MODERATE, 10,
            Seq("Stage 10 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 10 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
              "Stage 10 has skew in task input bytes (median is 50 MB, max is 250 MB).",
              "Stage 10: please set DaliSpark.SPLIT_SIZE to make partitions more even."))
          .spill(Severity.SEVERE, Severity.NONE, 0, 250, 2L << 20,
            Seq("Stage 10 is processing a lot of data; examine the application to see if this can be reduced.",
              "Stage 10 has 2 TB execution memory spill.",
              "Stage 10 has 5 tasks, 6 TB input read, 0 B shuffle read, 0 B shuffle write, 0 B output.",
              "Stage 10 has median task values: 50 MB memory spill, 50 MB input, 0 B shuffle read, 0 B shuffle write, 0 B output."))
          .create(),
        StageAnalysisBuilder(11, 3).taskRuntime(50, 250).input(6 << 20)
          .skew(Severity.MODERATE, Severity.MODERATE, 6,
            Seq("Stage 11 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
              "Stage 11 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
              "Stage 11 has skew in task input bytes (median is 50 MB, max is 250 MB).",
              "Stage 11: please set DaliSpark.SPLIT_SIZE to make partitions more even."))
          .spill(Severity.CRITICAL, Severity.NONE, 0, 250, 3L << 20,
            Seq("Stage 11 is processing a lot of data; examine the application to see if this can be reduced.",
              "Stage 11 has 3 TB execution memory spill.",
              "Stage 11 has 3 tasks, 6 TB input read, 0 B shuffle read, 0 B shuffle write, 0 B output.",
              "Stage 11 has median task values: 50 MB memory spill, 50 MB input, 0 B shuffle read, 0 B shuffle write, 0 B output."))
          .create(),
        StageAnalysisBuilder(12, 3).taskRuntime(50, 250)
          .skew(Severity.MODERATE, Severity.MODERATE, 6,
            Seq("Stage 12 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
            "Stage 12 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
            "Stage 12 has skew in task output bytes (median is 50 MB, max is 250 MB).",
            "Stage 12: please try to modify the application to make the partitions more even."))
          .spill(Severity.CRITICAL, Severity.NONE, 0, 250, 4L << 20,
            Seq("Stage 12 is processing a lot of data; examine the application to see if this can be reduced.",
            "Stage 12 has 4 TB execution memory spill.",
            "Stage 12 has 3 tasks, 0 B input read, 0 B shuffle read, 0 B shuffle write, 6 TB output.",
            "Stage 12 has median task values: 50 MB memory spill, 0 B input, 0 B shuffle read, 0 B shuffle write, 50 MB output."))
          .create())

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis()
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
    }
  }

  /** compare actual and expected StageAnalysis */
  private def compareStageAnalysis(actual: StageAnalysis, expected: StageAnalysis): Unit = {
    compareExecutionMemorySpillResult(actual.executionMemorySpillResult, expected.executionMemorySpillResult)
    compareLongTaskResult(actual.longTaskResult, expected.longTaskResult)
    compareTaskSkewResult(actual.taskSkewResult, expected.taskSkewResult)
    compareTaskFailureResult(actual.taskFailureResult, expected.taskFailureResult)
    compareStageFailureResult(actual.stageFailureResult, expected.stageFailureResult)
    compareStageGCResult(actual.stageGCResult, expected.stageGCResult)
  }

  /** compare actual and expected ExecutionMemorySpillResult */
  private def compareExecutionMemorySpillResult(
      actual: ExecutionMemorySpillResult,
      expected: ExecutionMemorySpillResult) = {
    actual.severity should be(expected.severity)
    actual.rawSeverity should be(expected.rawSeverity)
    actual.score should be(expected.score)
    actual.memoryBytesSpilled should be(expected.memoryBytesSpilled)
    actual.maxTaskBytesSpilled should be(expected.maxTaskBytesSpilled)
    actual.inputBytes should be(expected.inputBytes)
    actual.details should be(expected.details)
  }

  /** compare actual and expected LongTaskResult */
  private def compareLongTaskResult(
      actual: LongTaskResult,
      expected: LongTaskResult) = {
    actual.severity should be(expected.severity)
    actual.score should be(expected.score)
    actual.medianRunTime should be(expected.medianRunTime)
    actual.details should be(expected.details)
  }

  /** compare actual and expected TaskSkewResult */
  private def compareTaskSkewResult(
      actual: TaskSkewResult,
      expected: TaskSkewResult) = {
    actual.severity should be(expected.severity)
    actual.rawSeverity should be(expected.rawSeverity)
    actual.score should be(expected.score)
    actual.medianRunTime should be(expected.medianRunTime)
    actual.maxRunTime should be(expected.maxRunTime)
    actual.stageDuration should be(expected.stageDuration)
    actual.details should be(expected.details)
  }

  /** compare actual and expected TaskFailureResult */
  private def compareTaskFailureResult(
      actual: TaskFailureResult,
      expected: TaskFailureResult) = {
    actual.severity should be(expected.severity)
    actual.oomSeverity should be(expected.oomSeverity)
    actual.containerKilledSeverity should be(expected.containerKilledSeverity)
    actual.score should be(expected.score)
    actual.numTasks should be(expected.numTasks)
    actual.numFailures should be(expected.numFailures)
    actual.numOOM should be(expected.numOOM)
    actual.numContainerKilled should be (expected.numContainerKilled)
    actual.details should be(expected.details)
  }

  /** compare actual and expected StageFailureResult */
  private def compareStageFailureResult(
       actual: StageFailureResult,
       expected: StageFailureResult) = {
    actual.severity should be(expected.severity)
    actual.score should be(expected.score)
    actual.details should be(expected.details)
  }

  /** compare actual and expected StageGCResult */
  private def compareStageGCResult(
      actual: StageGCResult,
      expected: StageGCResult) = {
    actual.severity should be(expected.severity)
    actual.score should be(expected.score)
    actual.details should be(expected.details)
  }
}
