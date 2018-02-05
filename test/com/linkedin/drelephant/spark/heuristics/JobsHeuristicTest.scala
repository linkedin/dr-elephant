/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.heuristics

import scala.collection.JavaConverters

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, JobDataImpl}
import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

/**
  * Test class for Jobs Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class JobsHeuristicTest extends FunSpec with Matchers {
  import JobsHeuristicTest._

  describe("JobsHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map(
        "job_failure_rate_severity_thresholds" -> "0.2,0.4,0.6,0.8",
        "job_task_failure_rate_severity_thresholds" -> "0.2,0.4,0.6,0.8"
      )
    )
    val jobsHeuristic = new JobsHeuristic(heuristicConfigurationData)
    val jobDatas = Seq(
      newFakeJobData(0, "foo", JobExecutionStatus.SUCCEEDED, numCompleteTasks = 10, numFailedTasks = 0),
      newFakeJobData(1, "bar", JobExecutionStatus.SUCCEEDED, numCompleteTasks = 8, numFailedTasks = 2),
      newFakeJobData(2, "baz", JobExecutionStatus.SUCCEEDED, numCompleteTasks = 6, numFailedTasks = 4),
      newFakeJobData(3, "aaa", JobExecutionStatus.FAILED, numCompleteTasks = 4, numFailedTasks = 6),
      newFakeJobData(4, "zzz", JobExecutionStatus.FAILED, numCompleteTasks = 2, numFailedTasks = 8)
    )

    describe(".apply") {
      val data = newFakeSparkApplicationData(jobDatas)
      val heuristicResult = jobsHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.CRITICAL)
      }

      it("returns the number of completed jobs") {
        heuristicResultDetails.get(0).getValue should be("3")
      }

      it("returns the number of failed jobs") {
        heuristicResultDetails.get(1).getValue should be("2")
      }

      it("returns the list of failed jobs") {
        heuristicResultDetails.get(2).getValue should be(
          s"""|job 3, aaa
              |job 4, zzz""".stripMargin
        )
      }

      it("returns the job failure rate") {
        heuristicResultDetails.get(3).getValue should be("0.400")
      }

      it("returns the list of jobs with high task failure rates") {
        heuristicResultDetails.get(4).getValue should be(
          s"""|job 3, aaa (task failure rate: 0.600)
              |job 4, zzz (task failure rate: 0.800)""".stripMargin
        )
      }
    }

    describe(".Evaluator") {
      import JobsHeuristic.Evaluator

      val data = newFakeSparkApplicationData(jobDatas)
      val evaluator = new Evaluator(jobsHeuristic, data)

      it("has the number of completed jobs") {
        evaluator.numCompletedJobs should be(3)
      }

      it("has the number of failed jobs") {
        evaluator.numFailedJobs should be(2)
      }

      it("has the list of failed jobs") {
        val jobIds = evaluator.failedJobs.map { _.jobId }
        jobIds should contain theSameElementsInOrderAs(Seq(3, 4))
      }

      it("has the job failure rate") {
        evaluator.jobFailureRate should be(Some(0.4D))
      }

      it("has the list of jobs with high task failure rates") {
        val jobIdsAndTaskFailureRates =
          evaluator.jobsWithHighTaskFailureRates.map { case (jobData, taskFailureRate) => (jobData.jobId, taskFailureRate) }
        jobIdsAndTaskFailureRates should contain theSameElementsInOrderAs(Seq((3, 0.6D), (4, 0.8D)))
      }

      it("computes the overall severity") {
        evaluator.severity should be(Severity.CRITICAL)
      }
    }
  }
}

object JobsHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeJobData(
    jobId: Int,
    name: String,
    status: JobExecutionStatus,
    numCompleteTasks: Int,
    numFailedTasks: Int
  ): JobDataImpl = new JobDataImpl(
    jobId,
    name,
    description = None,
    submissionTime = None,
    completionTime = None,
    stageIds = Seq.empty,
    jobGroup = None,
    status,
    numTasks = numCompleteTasks + numFailedTasks,
    numActiveTasks = 0,
    numCompleteTasks,
    numSkippedTasks = 0,
    numFailedTasks,
    numActiveStages = 0,
    numCompletedStages = 0,
    numSkippedStages = 0,
    numFailedStages = 0
  )

  def newFakeSparkApplicationData(jobDatas: Seq[JobDataImpl]): SparkApplicationData = {
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas,
      stageDatas = Seq.empty,
      executorSummaries = Seq.empty,
      stagesWithFailedTasks = Seq.empty
    )

    SparkApplicationData(appId, restDerivedData, logDerivedData = None)
  }
}
