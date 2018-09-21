package com.linkedin.drelephant.spark.heuristics

import java.util.Date

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkRestDerivedData}
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
        StageBuilder(5, 4).failures(2, 0, 0).status(StageStatus.FAILED, Some("array issues")).create()

      )
      val data = createSparkApplicationData(stages, Seq.empty, None)

      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 3).create(),
        StageAnalysisBuilder(2, 5)
          .failures(Severity.CRITICAL, Severity.NONE, Severity.CRITICAL, Severity.NONE, 2, 2, 0)
          .create(),
        StageAnalysisBuilder(3, 15)
          .failures(Severity.NONE, Severity.SEVERE, Severity.CRITICAL, Severity.NONE, 2, 0, 1)
          .create(),
        StageAnalysisBuilder(4, 15)
          .failures(Severity.SEVERE, Severity.CRITICAL, Severity.CRITICAL, Severity.NONE, 3, 1, 2)
          .create(),
        StageAnalysisBuilder(5, 4)
          .failures(Severity.NONE, Severity.NONE, Severity.CRITICAL, Severity.CRITICAL, 2, 0, 0)
          .create()
      )
      val expectedDetails = List(
        "Stage 2: has 2 tasks that failed because of OutOfMemory exception.",
        "Stage 3: has 1 tasks that failed because the container was killed by YARN for exeeding memory limits.",
        "Stage 4: has 1 tasks that failed because of OutOfMemory exception.",
        "Stage 4: has 2 tasks that failed because the container was killed by YARN for exeeding memory limits.",
        "Stage 5 failed: array issues")

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis(200)
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }

      val details = stageAnalysis.flatMap(_.details)
      details should be(expectedDetails)
    }

    it("has task skew severity") {
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 5).taskRuntime(200, 250, 600).create(),
        StageBuilder(3, 5).taskRuntime(100, 250, 260).input(5, 250, 260).create(),
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

      val data = createSparkApplicationData(stages, Seq.empty, None)
      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 5).taskRuntime(200, 250)
            .longSeverity(Severity.LOW).create(),
        StageAnalysisBuilder(3, 5).taskRuntime(100, 250).input(260)
          .skewSeverity(Severity.LOW, Severity.LOW).create(),
        StageAnalysisBuilder(3, 5).taskRuntime(20, 250)
          .skewSeverity(Severity.SEVERE, Severity.SEVERE).create(),
        StageAnalysisBuilder(4, 5).taskRuntime(5, 250).input(260)
          .skewSeverity(Severity.CRITICAL, Severity.CRITICAL).create(),
        StageAnalysisBuilder(5, 5).taskRuntime(50, 250)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE).create(),
        StageAnalysisBuilder(6, 5).taskRuntime(50, 250)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE).create(),
        StageAnalysisBuilder(7, 5).taskRuntime(20, 250)
          .skewSeverity(Severity.SEVERE, Severity.SEVERE).create(),
        StageAnalysisBuilder(8, 3).taskRuntime(200, 250)
            .longSeverity(Severity.LOW).create(),
        StageAnalysisBuilder(9, 3).taskRuntime(5, 250)
          .skewSeverity(Severity.CRITICAL, Severity.CRITICAL).create(),
        StageAnalysisBuilder(10, 3).taskRuntime(20, 250).input(300)
          .skewSeverity(Severity.SEVERE, Severity.SEVERE).create(),
        StageAnalysisBuilder(11, 3).taskRuntime(50, 250)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE).create(),
        StageAnalysisBuilder(12, 5).taskRuntime(2, 50).duration(60)
          .skewSeverity(Severity.CRITICAL, Severity.NONE).create(),
        StageAnalysisBuilder(13, 5).taskRuntime(5, 50).input(600)
          .skewSeverity(Severity.SEVERE, Severity.NONE).create(),
        StageAnalysisBuilder(14, 5).taskRuntime(5, 200)
          .skewSeverity(Severity.CRITICAL, Severity.NONE).create())
      val expectedDetails = List(
        "Stage 3 has skew in task run time (median is 20.00 sec, max is 4.17 min)",
        "Stage 3: please try to modify the application to make the partitions more even.",
        "Stage 4 has skew in task run time (median is 5.00 sec, max is 4.17 min)",
        "Stage 4 has skew in task input bytes (median is 5 MB, max is 250 MB).",
        "Stage 4: please set DaliSpark.SPLIT_SIZE to make partitions more even.",
        "Stage 5 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 5 has skew in task shuffle read bytes (median is 50 MB, max is 250 MB).",
        "Stage 5 has skew in task shuffle write bytes (median is 50 MB, max is 250 MB).",
        "Stage 5: please try to modify the application to make the partitions more even.",
        "Stage 6 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 6: please try to modify the application to make the partitions more even.",
        "Stage 7 has skew in task run time (median is 20.00 sec, max is 4.17 min)",
        "Stage 7 has skew in task output bytes (median is 20 MB, max is 250 MB).",
        "Stage 7: please try to modify the application to make the partitions more even.",
        "Stage 9 has skew in task run time (median is 5.00 sec, max is 4.17 min)",
        "Stage 9: please try to modify the application to make the partitions more even.",
        "Stage 10 has skew in task run time (median is 20.00 sec, max is 4.17 min)",
        "Stage 10 has skew in task input bytes (median is 20 MB, max is 250 MB).",
        "Stage 10: please set DaliSpark.SPLIT_SIZE to make partitions more even.",
        "Stage 11 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 11 has skew in task shuffle read bytes (median is 50 MB, max is 250 MB).",
        "Stage 11: please try to modify the application to make the partitions more even.")

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis(5)
      val details = stageAnalysis.flatMap(_.details)
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
      details should be(expectedDetails)
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
      val data = createSparkApplicationData(stages, Seq.empty, None)
      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 3).taskRuntime(120, 150).create(),
        StageAnalysisBuilder(2, 3).taskRuntime(180, 200).longSeverity(Severity.LOW).create(),
        StageAnalysisBuilder(3, 3).taskRuntime(400, 500).longSeverity(Severity.MODERATE).create(),
        StageAnalysisBuilder(4, 3).taskRuntime(700, 900).longSeverity(Severity.SEVERE).create(),
        StageAnalysisBuilder(5, 3).taskRuntime(1200, 1500).longSeverity(Severity.CRITICAL).create(),
        StageAnalysisBuilder(6, 3).taskRuntime(700, 3500).longSeverity(Severity.SEVERE)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE).create(),
        StageAnalysisBuilder(7, 2).taskRuntime(700, 900).longSeverity(Severity.SEVERE).create(),
        StageAnalysisBuilder(8, 3).taskRuntime(3000, 3000).longSeverity(Severity.CRITICAL).input(5 << 20).create())

      val expectedDetails = List(
        "Stage 3 median task run time is 6.67 min.",
        "Stage 4 median task run time is 11.67 min.",
        "Stage 5 median task run time is 20.00 min.",
        "Stage 6 median task run time is 11.67 min.",
        "Stage 6 has skew in task run time (median is 11.67 min, max is 58.33 min)",
        "Stage 6: please try to modify the application to make the partitions more even.",
        "Stage 7 median task run time is 11.67 min.",
        "Stage 7: please increase the number of partitions, which is currently set to 2.",
        "Stage 8 is processing a lot of data; examine the application to see if this can be reduced.",
        "Stage 8 median task run time is 50.00 min.",
        "Stage 8: please set DaliSpark.SPLIT_SIZE to a smaller value to increase the number of" +
          " tasks reading input data for this stage.")

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis(3)
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
      val details = stageAnalysis.flatMap(_.details)
      details should be(expectedDetails)
    }

    it("has execution spill severity") {
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
      val data = createSparkApplicationData(stages, Seq.empty, None)
      val expectedAnalysis = Seq(
        StageAnalysisBuilder(1, 5).taskRuntime(100, 150)
          .spill(Severity.NONE, Severity.NONE, 2, 5).create(),
        StageAnalysisBuilder(2, 5).taskRuntime(100, 150)
          .spill(Severity.LOW, Severity.LOW, 15, 40).create(),
        StageAnalysisBuilder(3, 5).taskRuntime(100, 150).input(3000)
          .spill(Severity.MODERATE, Severity.MODERATE, 150, 400).create(),
        StageAnalysisBuilder(4, 5).taskRuntime(300, 350).longSeverity(Severity.MODERATE)
          .spill(Severity.SEVERE, Severity.SEVERE, 350, 1500).create(),
        StageAnalysisBuilder(5, 5).taskRuntime(300, 2500).longSeverity(Severity.MODERATE)
            .skewSeverity(Severity.SEVERE, Severity.SEVERE)
          .spill(Severity.MODERATE, Severity.MODERATE, 2500, 3000).create(),
        StageAnalysisBuilder(6, 3).taskRuntime(50, 250).input(350)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE)
          .spill(Severity.CRITICAL, Severity.CRITICAL, 250, 750).create(),
        StageAnalysisBuilder(7, 3).taskRuntime(50, 250)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE)
          .spill(Severity.CRITICAL, Severity.CRITICAL, 250, 750).create(),
        StageAnalysisBuilder(8, 5).taskRuntime(2, 50).duration(60)
          .skewSeverity(Severity.CRITICAL, Severity.NONE)
          .spill(Severity.CRITICAL, Severity.CRITICAL, 250, 750).create(),
        StageAnalysisBuilder(9, 5).taskRuntime(50, 250)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE)
          .spill(Severity.SEVERE, Severity.NONE, 250, 2L << 20).create(),
        StageAnalysisBuilder(10, 5).taskRuntime(50, 250).input(6 << 20)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE)
          .spill(Severity.SEVERE, Severity.NONE, 250, 2L << 20).create(),
        StageAnalysisBuilder(11, 3).taskRuntime(50, 250).input(6 << 20)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE)
          .spill(Severity.CRITICAL, Severity.NONE, 250, 3L << 20).create(),
        StageAnalysisBuilder(12, 3).taskRuntime(50, 250)
          .skewSeverity(Severity.MODERATE, Severity.MODERATE)
          .spill(Severity.CRITICAL, Severity.NONE, 250, 4L << 20).create())
      val expectedDetails = List("Stage 3 has 400 MB execution memory spill.",
        "Stage 3 has skew in task input bytes (median is 500 MB, max is 1.95 GB).",
        "Stage 3: please set DaliSpark.SPLIT_SIZE to make partitions more even.",
        "Stage 4 has 1.46 GB execution memory spill.",
        "Stage 4 median task run time is 5.00 min.",
        "Stage 4: please try to modify the application to make the partitions more even.",
        "Stage 5 has 2.93 GB execution memory spill.",
        "Stage 5 median task run time is 5.00 min.",
        "Stage 5 has skew in task run time (median is 5.00 min, max is 41.67 min)",
        "Stage 5 has skew in memory bytes spilled (median is 300 MB, max is 2.44 GB).",
        "Stage 5 has skew in task shuffle read bytes (median is 1,000 MB, max is 4.88 GB).",
        "Stage 5 has skew in task shuffle write bytes (median is 300 MB, max is 2.44 GB).",
        "Stage 5: please try to modify the application to make the partitions more even.",
        "Stage 6 has 750 MB execution memory spill.",
        "Stage 6 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 6 has skew in task input bytes (median is 50 MB, max is 250 MB).",
        "Stage 6: please set DaliSpark.SPLIT_SIZE to make partitions more even.",
        "Stage 7 has 750 MB execution memory spill.",
        "Stage 7 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 7 has skew in task output bytes (median is 250 MB, max is 1,000 MB).",
        "Stage 7: please try to modify the application to make the partitions more even.",
        "Stage 8 has 750 MB execution memory spill.",
        "Stage 8: please try to modify the application to make the partitions more even.",
        "Stage 9 is processing a lot of data; examine the application to see if this can be reduced.",
        "Stage 9 has 2 TB execution memory spill.",
        "Stage 9 has 5 tasks, 0 B input read, 0 B shuffle read, 0 B shuffle write, 6 TB output.",
        "Stage 9 has median task values: 50 MB memory spill, 0 B input, 0 B shuffle read, 0 B shuffle write, 50 MB output.",
        "Stage 9 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 9 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
        "Stage 9 has skew in task output bytes (median is 50 MB, max is 250 MB).",
        "Stage 9: please try to modify the application to make the partitions more even.",
        "Stage 10 is processing a lot of data; examine the application to see if this can be reduced.",
        "Stage 10 has 2 TB execution memory spill.",
        "Stage 10 has 5 tasks, 6 TB input read, 0 B shuffle read, 0 B shuffle write, 0 B output.",
        "Stage 10 has median task values: 50 MB memory spill, 50 MB input, 0 B shuffle read, 0 B shuffle write, 0 B output.",
        "Stage 10 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 10 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
        "Stage 10 has skew in task input bytes (median is 50 MB, max is 250 MB).",
        "Stage 10: please set DaliSpark.SPLIT_SIZE to make partitions more even.",
        "Stage 11 is processing a lot of data; examine the application to see if this can be reduced.",
        "Stage 11 has 3 TB execution memory spill.",
        "Stage 11 has 3 tasks, 6 TB input read, 0 B shuffle read, 0 B shuffle write, 0 B output.",
        "Stage 11 has median task values: 50 MB memory spill, 50 MB input, 0 B shuffle read, 0 B shuffle write, 0 B output.",
        "Stage 11 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 11 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
        "Stage 11 has skew in task input bytes (median is 50 MB, max is 250 MB).",
        "Stage 11: please set DaliSpark.SPLIT_SIZE to make partitions more even.",
        "Stage 12 is processing a lot of data; examine the application to see if this can be reduced.",
        "Stage 12 has 4 TB execution memory spill.",
        "Stage 12 has 3 tasks, 0 B input read, 0 B shuffle read, 0 B shuffle write, 6 TB output.",
        "Stage 12 has median task values: 50 MB memory spill, 0 B input, 0 B shuffle read, 0 B shuffle write, 50 MB output.",
        "Stage 12 has skew in task run time (median is 50.00 sec, max is 4.17 min)",
        "Stage 12 has skew in memory bytes spilled (median is 50 MB, max is 250 MB).",
        "Stage 12 has skew in task output bytes (median is 50 MB, max is 250 MB).",
        "Stage 12: please try to modify the application to make the partitions more even.")

      val stageAnalyzer = new StagesAnalyzer(heuristicConfigurationData, data)
      val stageAnalysis = stageAnalyzer.getStageAnalysis(5)
      val details = stageAnalysis.flatMap(_.details)
      (0 until expectedAnalysis.size).foreach { i =>
        compareStageAnalysis(stageAnalysis(i), expectedAnalysis(i))
      }
      details should be(expectedDetails)

    }
  }

  private def compareStageAnalysis(actual: StageAnalysis, expected: StageAnalysis): Unit = {
    actual.stageId should be(expected.stageId)
    actual.stageDuration should be(expected.stageDuration)
    actual.rawSkewSeverity should be(expected.rawSkewSeverity)
    actual.taskSkewSeverity should be(expected.taskSkewSeverity)
    actual.longTaskSeverity should be(expected.longTaskSeverity)
    actual.rawSpillSeverity should be(expected.rawSpillSeverity)
    actual.executionSpillSeverity should be(expected.executionSpillSeverity)
    actual.failedWithOOMSeverity should be(expected.failedWithOOMSeverity)
    actual.failedWithContainerKilledSeverity should be(expected.failedWithContainerKilledSeverity)
    actual.taskFailureSeverity should be(expected.taskFailureSeverity)
    actual.stageFailureSeverity should be(expected.stageFailureSeverity)
    actual.gcSeverity should be(expected.gcSeverity)
    actual.maxTaskBytesSpilled should be(expected.maxTaskBytesSpilled)
    actual.memoryBytesSpilled should be(expected.memoryBytesSpilled)
    actual.numTasks should be(expected.numTasks)
    actual.numFailedTasks should be(expected.numFailedTasks)
    actual.numTasksWithContainerKilled should be (expected.numTasksWithContainerKilled)
    actual.numTasksWithOOM should be (expected.numTasksWithOOM)
    actual.medianRunTime should be (expected.medianRunTime)
    actual.maxRunTime should be (expected.maxRunTime)
    actual.inputBytes should be (expected.inputBytes)
  }
}
