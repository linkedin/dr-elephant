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

package com.linkedin.drelephant.spark.legacydata

import java.util.Date

import org.apache.spark.JobExecutionStatus
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import org.scalatest.{FunSpec, Matchers}

class LegacyDataConvertersTest extends FunSpec with Matchers {
  describe("LegacyDataConverters") {
    describe(".convert") {
    }

    describe(".extractAppConfigurationProperties") {
      it("returns a Map of Spark properties extracted from the given legacy SparkApplicationData") {
        val legacyData = new MockSparkApplicationData() {
          val environmentData = {
            val environmentData = new SparkEnvironmentData()
            environmentData.addSparkProperty("a", "b")
            environmentData.addSparkProperty("c", "d")
            environmentData
          }

          override def getEnvironmentData(): SparkEnvironmentData = environmentData
        }

        val appConfigurationProperties = LegacyDataConverters.extractAppConfigurationProperties(legacyData)
        appConfigurationProperties should contain theSameElementsAs Map("a" -> "b", "c" -> "d")
      }
    }

    describe(".extractApplicationInfo") {
      it("returns an ApplicationInfo extracted from the given legacy SparkApplicationData") {
        val legacyData = new MockSparkApplicationData() {
          val generalData = {
            val generalData = new SparkGeneralData()
            generalData.setApplicationId("application_1")
            generalData.setApplicationName("app")
            generalData.setStartTime(1000L)
            generalData.setEndTime(2000L)
            generalData.setSparkUser("foo")
            generalData
          }

          override def getGeneralData(): SparkGeneralData = generalData
        }

        val applicationInfo = LegacyDataConverters.extractApplicationInfo(legacyData)
        applicationInfo.id should be("application_1")
        applicationInfo.name should be("app")
        applicationInfo.attempts.size should be(1)

        val applicationAttemptInfo = applicationInfo.attempts.last
        applicationAttemptInfo.attemptId should be(Some("1"))
        applicationAttemptInfo.startTime should be(new Date(1000L))
        applicationAttemptInfo.endTime should be(new Date(2000L))
        applicationAttemptInfo.sparkUser should be("foo")
        applicationAttemptInfo.completed should be(true)
      }
    }

    describe(".extractJobDatas") {
      it("returns JobDatas extracted from the given legacy SparkApplicationData") {
        val legacyData = new MockSparkApplicationData() {
          val jobProgressData = {
            val jobProgressData = new SparkJobProgressData()

            val jobInfo1 = {
              val jobInfo = new SparkJobProgressData.JobInfo()
              jobInfo.jobId = 1

              jobInfo.numTasks = 10
              jobInfo.numActiveTasks = 1
              jobInfo.numCompletedTasks = 2
              jobInfo.numSkippedTasks = 3
              jobInfo.numFailedTasks = 4

              for (i <- 1 to 100) { jobInfo.stageIds.add(i) }
              jobInfo.numActiveStages = 10
              for (i <- 1 to 20) { jobInfo.completedStageIndices.add(i) }
              jobInfo.numSkippedStages = 30
              jobInfo.numFailedStages = 40

              jobInfo
            }
            jobProgressData.addJobInfo(1, jobInfo1)
            jobProgressData.addCompletedJob(1)

            val jobInfo2 = {
              val jobInfo = new SparkJobProgressData.JobInfo()
              jobInfo.jobId = 2
              jobInfo
            }
            jobProgressData.addJobInfo(2, jobInfo2)
            jobProgressData.addFailedJob(2)

            jobProgressData
          }

          override def getJobProgressData(): SparkJobProgressData = jobProgressData
        }

        val jobDatas = LegacyDataConverters.extractJobDatas(legacyData)
        jobDatas.size should be(2)

        val jobData1 = jobDatas(0)
        jobData1.jobId should be(1)
        jobData1.name should be("1")
        jobData1.description should be(None)
        jobData1.submissionTime should be(None)
        jobData1.completionTime should be(None)
        jobData1.stageIds should be((1 to 100).toSeq)
        jobData1.jobGroup should be(None)
        jobData1.status should be(JobExecutionStatus.SUCCEEDED)
        jobData1.numTasks should be(10)
        jobData1.numActiveTasks should be(1)
        jobData1.numCompletedTasks should be(2)
        jobData1.numSkippedTasks should be(3)
        jobData1.numFailedTasks should be(4)
        jobData1.numActiveStages should be(10)
        jobData1.numCompletedStages should be(20)
        jobData1.numSkippedStages should be(30)
        jobData1.numFailedStages should be(40)

        val jobData2 = jobDatas(1)
        jobData2.jobId should be(2)
        jobData2.name should be("2")
        jobData2.status should be(JobExecutionStatus.FAILED)
      }
    }

    describe(".extractStageDatas") {
      it("returns StageDatas extracted from the given legacy SparkApplicationData") {
        val legacyData = new MockSparkApplicationData() {
          val jobProgressData = {
            val jobProgressData = new SparkJobProgressData()

            val stageInfoS1A1 = {
              val stageInfo = new SparkJobProgressData.StageInfo()

              stageInfo.numActiveTasks = 1
              stageInfo.numCompleteTasks = 2
              stageInfo.numFailedTasks = 3

              stageInfo.executorRunTime = 1000L

              stageInfo.inputBytes = 10000L
              stageInfo.outputBytes = 20000L
              stageInfo.shuffleReadBytes = 30000L
              stageInfo.shuffleWriteBytes = 40000L
              stageInfo.memoryBytesSpilled = 50000L
              stageInfo.diskBytesSpilled = 60000L

              stageInfo.name = "1,1"
              stageInfo.description = "a"

              stageInfo
            }
            jobProgressData.addStageInfo(1, 1, stageInfoS1A1)
            jobProgressData.addCompletedStages(1, 1)

            val stageInfoS1A2 = {
              val stageInfo = new SparkJobProgressData.StageInfo()
              stageInfo.name = "1,2"
              stageInfo
            }
            jobProgressData.addStageInfo(1, 2, stageInfoS1A2)
            jobProgressData.addCompletedStages(1, 2)

            val stageInfoS2A1 = {
              val stageInfo = new SparkJobProgressData.StageInfo()
              stageInfo.name = "2,1"
              stageInfo
            }
            jobProgressData.addStageInfo(2, 1, stageInfoS2A1)
            jobProgressData.addFailedStages(2, 1)

            jobProgressData
          }

          override def getJobProgressData(): SparkJobProgressData = jobProgressData
        }

        val stageDatas = LegacyDataConverters.extractStageDatas(legacyData)
        stageDatas.size should be(3)

        val stageDataS1A1 = stageDatas(0)
        stageDataS1A1.status should be(StageStatus.COMPLETE)
        stageDataS1A1.stageId should be(1)
        stageDataS1A1.attemptId should be(1)
        stageDataS1A1.numActiveTasks should be(1)
        stageDataS1A1.numCompleteTasks should be(2)
        stageDataS1A1.numFailedTasks should be(3)
        stageDataS1A1.executorRunTime should be(1000L)
        stageDataS1A1.inputBytes should be(10000L)
        stageDataS1A1.inputRecords should be(0L)
        stageDataS1A1.outputBytes should be(20000L)
        stageDataS1A1.outputRecords should be(0L)
        stageDataS1A1.shuffleReadBytes should be(30000L)
        stageDataS1A1.shuffleReadRecords should be(0L)
        stageDataS1A1.shuffleWriteBytes should be(40000L)
        stageDataS1A1.shuffleWriteRecords should be(0L)
        stageDataS1A1.memoryBytesSpilled should be(50000L)
        stageDataS1A1.diskBytesSpilled should be(60000L)
        stageDataS1A1.name should be("1,1")
        stageDataS1A1.details should be("a")
        stageDataS1A1.schedulingPool should be("")
        stageDataS1A1.accumulatorUpdates should be(Seq.empty)
        stageDataS1A1.tasks should be(None)
        stageDataS1A1.executorSummary should be(None)

        val stageDataS1A2 = stageDatas(1)
        stageDataS1A2.status should be(StageStatus.COMPLETE)
        stageDataS1A2.stageId should be(1)
        stageDataS1A2.attemptId should be(2)
        stageDataS1A2.name should be("1,2")

        val stageDataS2A1 = stageDatas(2)
        stageDataS2A1.status should be(StageStatus.FAILED)
        stageDataS2A1.stageId should be(2)
        stageDataS2A1.attemptId should be(1)
        stageDataS2A1.name should be("2,1")
      }
    }

    describe(".extractExecutorSummaries") {
      it("returns ExecutorSummaries extracted from the given legacy SparkApplicationData") {
        val legacyData = new MockSparkApplicationData() {
          val executorData = {
            val executorData = new SparkExecutorData()

            val executorInfo1 = {
              val executorInfo = new SparkExecutorData.ExecutorInfo()

              executorInfo.execId = "1"
              executorInfo.hostPort = "9090"

              executorInfo.rddBlocks = 10
              executorInfo.memUsed = 10000L
              executorInfo.maxMem = 20000L
              executorInfo.diskUsed = 30000L

              executorInfo.activeTasks = 1
              executorInfo.completedTasks = 2
              executorInfo.failedTasks = 3
              executorInfo.totalTasks = 6

              executorInfo.duration = 1000L

              executorInfo.inputBytes = 100000L
              executorInfo.shuffleRead = 200000L
              executorInfo.shuffleWrite = 300000L

              executorInfo
            }
            executorData.setExecutorInfo("1", executorInfo1)

            val executorInfo2 = {
              val executorInfo = new SparkExecutorData.ExecutorInfo()
              executorInfo.execId = "2"
              executorInfo
            }
            executorData.setExecutorInfo("2", executorInfo2)

            executorData
          }

          override def getExecutorData(): SparkExecutorData = executorData
        }

        val executorSummaries = LegacyDataConverters.extractExecutorSummaries(legacyData)
        executorSummaries.size should be(2)

        val executorSummary1 = executorSummaries(0)
        executorSummary1.id should be("1")
        executorSummary1.hostPort should be("9090")
        executorSummary1.rddBlocks should be(10)
        executorSummary1.memoryUsed should be(10000L)
        executorSummary1.diskUsed should be(30000L)
        executorSummary1.activeTasks should be(1)
        executorSummary1.failedTasks should be(3)
        executorSummary1.completedTasks should be(2)
        executorSummary1.totalTasks should be(6)
        executorSummary1.totalDuration should be(1000L)
        executorSummary1.totalInputBytes should be(100000L)
        executorSummary1.totalShuffleRead should be(200000L)
        executorSummary1.totalShuffleWrite should be(300000L)
        executorSummary1.maxMemory should be(20000L)
        executorSummary1.executorLogs should be(Map.empty)

        val executorSummary2 = executorSummaries(1)
        executorSummary2.id should be("2")
      }
    }

    describe(".") {
    }
  }
}

object LegacyDataConvertersTest {

}
