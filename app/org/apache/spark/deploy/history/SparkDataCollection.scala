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

package org.apache.spark.deploy.history

import java.io.InputStream
import java.util.{Set => JSet, Properties, List => JList, HashSet => JHashSet, ArrayList => JArrayList}

import scala.collection.mutable

import com.linkedin.drelephant.analysis.ApplicationType
import com.linkedin.drelephant.spark.legacydata._
import com.linkedin.drelephant.spark.legacydata.SparkExecutorData.ExecutorInfo
import com.linkedin.drelephant.spark.legacydata.SparkJobProgressData.JobInfo

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{ApplicationEventListener, ReplayListenerBus, StageInfo}
import org.apache.spark.storage.{RDDInfo, StorageStatus, StorageStatusListener, StorageStatusTrackingListener}
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.storage.StorageListener
import org.apache.spark.util.collection.OpenHashSet

/**
 * This class wraps the logic of collecting the data in SparkEventListeners into the
 * HadoopApplicationData instances.
 *
 * Notice:
 * This has to live in Spark's scope because ApplicationEventListener is in private[spark] scope. And it is problematic
 * to compile if written in Java.
 */
class SparkDataCollection extends SparkApplicationData {
  import SparkDataCollection._

  lazy val applicationEventListener = new ApplicationEventListener()
  lazy val jobProgressListener = new JobProgressListener(new SparkConf())
  lazy val environmentListener = new EnvironmentListener()
  lazy val storageStatusListener = new StorageStatusListener(new SparkConf())
  lazy val executorsListener = new ExecutorsListener(storageStatusListener, new SparkConf())
  lazy val storageListener = new StorageListener(storageStatusListener)

  // This is a customized listener that tracks peak used memory
  // The original listener only tracks the current in use memory which is useless in offline scenario.
  lazy val storageStatusTrackingListener = new StorageStatusTrackingListener()

  private var _applicationData: SparkGeneralData = null;
  private var _jobProgressData: SparkJobProgressData = null;
  private var _environmentData: SparkEnvironmentData = null;
  private var _executorData: SparkExecutorData = null;
  private var _storageData: SparkStorageData = null;
  private var _isThrottled: Boolean = false;

  def throttle(): Unit = {
    _isThrottled = true
  }

  override def isThrottled(): Boolean = _isThrottled

  override def getApplicationType(): ApplicationType = APPLICATION_TYPE

  override def getConf(): Properties = getEnvironmentData().getSparkProperties()

  override def isEmpty(): Boolean = !isThrottled() && getExecutorData().getExecutors.isEmpty()

  override def getGeneralData(): SparkGeneralData = {
    if (_applicationData == null) {
      _applicationData = new SparkGeneralData()

      applicationEventListener.adminAcls match {
        case Some(s: String) => {
          _applicationData.setAdminAcls(stringToSet(s))
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.viewAcls match {
        case Some(s: String) => {
          _applicationData.setViewAcls(stringToSet(s))
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.appId match {
        case Some(s: String) => {
          _applicationData.setApplicationId(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.appName match {
        case Some(s: String) => {
          _applicationData.setApplicationName(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.sparkUser match {
        case Some(s: String) => {
          _applicationData.setSparkUser(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.startTime match {
        case Some(s: Long) => {
          _applicationData.setStartTime(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.endTime match {
        case Some(s: Long) => {
          _applicationData.setEndTime(s)
        }
        case None => {
          // do nothing
        }
      }
    }
    _applicationData
  }

  override def getEnvironmentData(): SparkEnvironmentData = {
    if (_environmentData == null) {
      // Notice: we ignore jvmInformation and classpathEntries, because they are less likely to be used by any analyzer.
      _environmentData = new SparkEnvironmentData()
      environmentListener.systemProperties.foreach { case (name, value) =>
        _environmentData.addSystemProperty(name, value)
                                                   }
      environmentListener.sparkProperties.foreach { case (name, value) =>
        _environmentData.addSparkProperty(name, value)
                                                  }
    }
    _environmentData
  }

  override def getExecutorData(): SparkExecutorData = {
    if (_executorData == null) {
      _executorData = new SparkExecutorData()

      for (statusId <- 0 until executorsListener.activeStorageStatusList.size) {
        val info = new ExecutorInfo()

        val status = executorsListener.activeStorageStatusList(statusId)

        info.execId = status.blockManagerId.executorId
        info.hostPort = status.blockManagerId.hostPort
        info.rddBlocks = status.numBlocks

        // Use a customized listener to fetch the peak memory used, the data contained in status are
        // the current used memory that is not useful in offline settings.
        info.memUsed = storageStatusTrackingListener.executorIdToMaxUsedMem.getOrElse(info.execId, 0L)
        info.maxMem = status.maxMem
        info.diskUsed = status.diskUsed

        val taskSummary = executorsListener.executorToTaskSummary.get(info.execId);

        if (!taskSummary.isEmpty) {
          info.activeTasks = taskSummary.get.tasksActive
          info.failedTasks = taskSummary.get.tasksFailed
          info.completedTasks = taskSummary.get.tasksComplete
          info.duration = taskSummary.get.duration
          info.inputBytes = taskSummary.get.inputBytes
          info.shuffleRead = taskSummary.get.shuffleRead
          info.shuffleWrite = taskSummary.get.shuffleWrite
        } else {
          info.activeTasks = 0
          info.failedTasks = 0
          info.completedTasks = 0
          info.duration = 0
          info.inputBytes = 0
          info.shuffleRead = 0
          info.shuffleWrite = 0
        }

        _executorData.setExecutorInfo(info.execId, info)
      }
    }
    _executorData
  }

  override def getJobProgressData(): SparkJobProgressData = {
    if (_jobProgressData == null) {
      _jobProgressData = new SparkJobProgressData()

      // Add JobInfo
      jobProgressListener.jobIdToData.foreach { case (id, data) =>
        val jobInfo = new JobInfo()

        jobInfo.jobId = data.jobId
        jobInfo.jobGroup = data.jobGroup.getOrElse("")
        jobInfo.numActiveStages = data.numActiveStages
        jobInfo.numActiveTasks = data.numActiveTasks
        jobInfo.numCompletedTasks = data.numCompletedTasks
        jobInfo.numFailedStages = data.numFailedStages
        jobInfo.numFailedTasks = data.numFailedTasks
        jobInfo.numSkippedStages = data.numSkippedStages
        jobInfo.numSkippedTasks = data.numSkippedTasks
        jobInfo.numTasks = data.numTasks

        jobInfo.startTime = data.submissionTime.getOrElse(0)
        jobInfo.endTime = data.completionTime.getOrElse(0)

        data.stageIds.foreach{ case (id: Int) => jobInfo.addStageId(id)}
        addIntSetToJSet(data.completedStageIndices, jobInfo.completedStageIndices)

        _jobProgressData.addJobInfo(id, jobInfo)
      }

      // Add Stage Info
      jobProgressListener.stageIdToData.foreach { case (id, data) =>
          val stageInfo = new SparkJobProgressData.StageInfo()
          val sparkStageInfo = jobProgressListener.stageIdToInfo.get(id._1)
          stageInfo.name = sparkStageInfo match {
            case Some(info: StageInfo) => {
              info.name
            }
            case None => {
              ""
            }
          }
          stageInfo.description = data.description.getOrElse("")
          stageInfo.diskBytesSpilled = data.diskBytesSpilled
          stageInfo.executorRunTime = data.executorRunTime
          stageInfo.duration = sparkStageInfo match {
            case Some(info: StageInfo) => {
              val submissionTime = info.submissionTime.getOrElse(0L)
              info.completionTime.getOrElse(submissionTime) - submissionTime
            }
            case _ => 0L
          }
          stageInfo.inputBytes = data.inputBytes
          stageInfo.memoryBytesSpilled = data.memoryBytesSpilled
          stageInfo.numActiveTasks = data.numActiveTasks
          stageInfo.numCompleteTasks = data.numCompleteTasks
          stageInfo.numFailedTasks = data.numFailedTasks
          stageInfo.outputBytes = data.outputBytes
          stageInfo.shuffleReadBytes = data.shuffleReadTotalBytes
          stageInfo.shuffleWriteBytes = data.shuffleWriteBytes
          addIntSetToJSet(data.completedIndices, stageInfo.completedIndices)

          _jobProgressData.addStageInfo(id._1, id._2, stageInfo)
      }

      // Add completed jobs
      jobProgressListener.completedJobs.foreach { case (data) => _jobProgressData.addCompletedJob(data.jobId) }
      // Add failed jobs
      jobProgressListener.failedJobs.foreach { case (data) => _jobProgressData.addFailedJob(data.jobId) }
      // Add completed stages
      jobProgressListener.completedStages.foreach { case (data) =>
        _jobProgressData.addCompletedStages(data.stageId, data.attemptId)
      }
      // Add failed stages
      jobProgressListener.failedStages.foreach { case (data) =>
        _jobProgressData.addFailedStages(data.stageId, data.attemptId)
      }
    }
    _jobProgressData
  }

  // This method returns a combined information from StorageStatusListener and StorageListener
  override def getStorageData(): SparkStorageData = {
    if (_storageData == null) {
      _storageData = new SparkStorageData()
      _storageData.setRddInfoList(toJList[RDDInfo](storageListener.rddInfoList))
      _storageData.setStorageStatusList(toJList[StorageStatus](storageStatusListener.storageStatusList))
    }
    _storageData
  }

  override def getAppId: String = {
    getGeneralData().getApplicationId
  }

  def load(in: InputStream, sourceName: String): Unit = {
    val replayBus = new ReplayListenerBus()
    replayBus.addListener(applicationEventListener)
    replayBus.addListener(jobProgressListener)
    replayBus.addListener(environmentListener)
    replayBus.addListener(storageStatusListener)
    replayBus.addListener(executorsListener)
    replayBus.addListener(storageListener)
    replayBus.addListener(storageStatusTrackingListener)

    // filter only for spark 2.x event log
    // ex. {"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
    //     {"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"
    //     {"Event":"org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates" ...
    replayBus.replay(in, sourceName, maybeTruncated = false, { (eventString: String) => {
      if (eventString.contains("\"Event\":\"org.apache.spark.sql.execution.ui.")) {
        false
      } else {
        true
      }
    }
    })
  }
}

object SparkDataCollection {
  private val APPLICATION_TYPE = new ApplicationType("SPARK")

  def stringToSet(str: String): JSet[String] = {
    val set = new JHashSet[String]()
    str.split(",").foreach { case t: String => set.add(t)}
    set
  }

  def toJList[T](seq: Seq[T]): JList[T] = {
    val list = new JArrayList[T]()
    seq.foreach { case (item: T) => list.add(item)}
    list
  }

  def addIntSetToJSet(set: OpenHashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }

  def addIntSetToJSet(set: mutable.HashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }
}
