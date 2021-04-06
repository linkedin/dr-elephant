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

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.util
import java.util.zip.ZipInputStream
import java.util.{Arrays, Properties, ArrayList => JArrayList, HashSet => JHashSet, List => JList, Set => JSet}

import com.alibaba.fastjson.JSONObject
import com.google.gson.Gson
import com.linkedin.drelephant.ElephantContext

import scala.collection.mutable
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.spark.fetchers.SparkRestClient.IN_PROGRESS
import com.linkedin.drelephant.spark.legacydata._
import com.linkedin.drelephant.spark.legacydata.SparkExecutorData.ExecutorInfo
import com.linkedin.drelephant.spark.legacydata.SparkJobProgressData.JobInfo
import com.linkedin.drelephant.util.SparkUtils
import models.AppResult
import org.apache.spark.{JobExecutionStatus, SparkConf, SparkContext}
import org.apache.spark.scheduler.{EventLoggingListener, SparkListenerApplicationStart, StatsReportListener}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, JobData, RDDStorageInfo, StageStatus}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{ReplayListenerBus, StageInfo}
import org.apache.spark.storage.{RDDInfo, StorageStatusTrackingListener}
import org.apache.spark.storage.StorageStatus
//import org.apache.spark.storage.StorageStatusListener
//import org.apache.spark.ui.env.EnvironmentListener
//import org.apache.spark.ui.exec.ExecutorsListener
//import org.apache.spark.ui.jobs.JobProgressListener
//import org.apache.spark.ui.storage.StorageListener
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

  lazy val appStatusStore = AppStatusStore.createLiveStore(new SparkConf())
  lazy val statsReportListener = new StatsReportListener()


/*  lazy val applicationEventListener = new ApplicationEventListener()
  lazy val jobProgressListener = new JobProgressListener(new SparkConf())
  lazy val environmentListener = new EnvironmentListener()
  lazy val storageStatusListener = new StorageStatusListener(new SparkConf())
  lazy val executorsListener = new ExecutorsListener(storageStatusListener, new SparkConf())
  lazy val storageListener = new StorageListener(storageStatusListener)*/

  // This is a customized listener that tracks peak used memory
  // The original listener only tracks the current in use memory which is useless in offline scenario.
//  lazy val storageStatusTrackingListener = new StorageStatusTrackingListener()

  private var _applicationData: SparkGeneralData = _
  private var _jobProgressData: SparkJobProgressData = _
  private var _environmentData: SparkEnvironmentData = _
  private var _executorData: SparkExecutorData = _
  private var _storageData: SparkStorageData = _
  private var _isThrottled: Boolean = false

  def throttle(): Unit = {
    _isThrottled = true
  }

  override def isThrottled: Boolean = _isThrottled

  override def getApplicationType(): ApplicationType = APPLICATION_TYPE

  override def getConf(): Properties = getEnvironmentData().getSparkProperties

  override def isEmpty(): Boolean = !isThrottled() && getExecutorData().getExecutors.isEmpty

  override def getGeneralData(): SparkGeneralData = {
    if (_applicationData == null) {
      _applicationData = new SparkGeneralData()

      val applicationInfo = appStatusStore.applicationInfo()

      _applicationData.setApplicationId(applicationInfo.id)
      _applicationData.setApplicationName(applicationInfo.name)


      println("memoryPerExecutorMB=" + applicationInfo.memoryPerExecutorMB.getOrElse(0))
      println("coresGranted=" + applicationInfo.coresGranted.getOrElse(0))
      println("coresPerExecutor=" + applicationInfo.coresPerExecutor.getOrElse(0))
      println("maxCores=" + applicationInfo.maxCores.getOrElse(0))

      println("attempts=" + applicationInfo.attempts.mkString("###"))
      applicationInfo.attempts.foreach { case (applicationAttemptInfo) =>

        println(applicationAttemptInfo.appSparkVersion + ", " + applicationAttemptInfo.attemptId + ", " +
          applicationAttemptInfo.completed + ", " + applicationAttemptInfo.duration+ ", " + applicationAttemptInfo.startTime
          + ", " + applicationAttemptInfo.endTime + ", " + applicationAttemptInfo.sparkUser)

        _applicationData.setSparkUser(applicationAttemptInfo.sparkUser)
        _applicationData.setStartTime(applicationAttemptInfo.startTime.getTime)
        _applicationData.setEndTime(applicationAttemptInfo.endTime.getTime)

      }

      _applicationData.setAdminAcls(stringToSet(""))
      _applicationData.setViewAcls(stringToSet(""))

      /*applicationEventListener.adminAcls match {
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
      }*/
    }
    _applicationData
  }

  override def getEnvironmentData(): SparkEnvironmentData = {
    if (_environmentData == null) {
      // Notice: we ignore jvmInformation and classpathEntries, because they are less likely to be used by any analyzer.
      _environmentData = new SparkEnvironmentData()
      /*environmentListener.systemProperties.foreach { case (name, value) =>
        _environmentData.addSystemProperty(name, value)
                                                   }
      environmentListener.sparkProperties.foreach { case (name, value) =>
        _environmentData.addSparkProperty(name, value)
                                                  }*/

      val environmentInfo = appStatusStore.environmentInfo();
      environmentInfo.systemProperties.foreach { case (name, value) =>
        _environmentData.addSystemProperty(name, value)
      }
      environmentInfo.sparkProperties.foreach { case (name, value) =>
        _environmentData.addSparkProperty(name, value)
      }
    }
    _environmentData
  }

  override def getExecutorData(): SparkExecutorData = {
    if (_executorData == null) {
      _executorData = new SparkExecutorData()

      val storageStatusList = appStatusStore.executorList(false)
      for (statusId <- 0 until storageStatusList.size) {
        val info = new ExecutorInfo()

        val status = storageStatusList(statusId)

        info.execId = status.id
        info.hostPort = status.hostPort
        info.rddBlocks = status.rddBlocks

        // Use a customized listener to fetch the peak memory used, the data contained in status are
        // the current used memory that is not useful in offline settings.
        info.memUsed = status.memoryUsed
        info.maxMem = status.maxMemory
        info.diskUsed = status.diskUsed
        if (!info.execId.equalsIgnoreCase("driver")) {

          info.activeTasks = status.activeTasks
          info.failedTasks = status.failedTasks
          info.completedTasks = status.completedTasks
          info.totalTasks = status.totalTasks
          info.duration = status.totalDuration
          info.inputBytes = status.totalInputBytes
          info.outputBytes = 0L
            //          info.outputBytes = executorsListener.executorToTaskSummary(info.execId).outputBytes
          info.shuffleRead = status.totalShuffleRead
          info.shuffleWrite = status.totalShuffleWrite
          info.totalGCTime = status.totalGCTime
        }

        _executorData.setExecutorInfo(info.execId, info)
      }
      /*val storageStatusList = executorsListener.activeStorageStatusList ++ executorsListener.deadStorageStatusList

      //      for (statusId <- 0 until executorsListener.storageStatusList.size) {
      for (statusId <- 0 until storageStatusList.size) {
        val info = new ExecutorInfo()

        val status = storageStatusList(statusId)

        info.execId = status.blockManagerId.executorId
        info.hostPort = status.blockManagerId.hostPort
        info.rddBlocks = status.numBlocks

        // Use a customized listener to fetch the peak memory used, the data contained in status are
        // the current used memory that is not useful in offline settings.
        info.memUsed = storageStatusTrackingListener.executorIdToMaxUsedMem.getOrElse(info.execId, 0L)
        info.maxMem = status.maxMem
        info.diskUsed = status.diskUsed
        if (!info.execId.equalsIgnoreCase("driver")) {
          info.activeTasks = executorsListener.executorToTaskSummary(info.execId).tasksActive
          info.failedTasks = executorsListener.executorToTaskSummary(info.execId).tasksFailed
          info.completedTasks = executorsListener.executorToTaskSummary(info.execId).tasksComplete
          info.totalTasks = info.activeTasks + info.failedTasks + info.completedTasks
          info.duration = executorsListener.executorToTaskSummary(info.execId).duration
          info.inputBytes = executorsListener.executorToTaskSummary(info.execId).inputBytes
          info.outputBytes = executorsListener.executorToTaskSummary(info.execId).outputBytes
          info.shuffleRead = executorsListener.executorToTaskSummary(info.execId).shuffleRead
          info.shuffleWrite = executorsListener.executorToTaskSummary(info.execId).shuffleWrite
          info.totalGCTime = executorsListener.executorToTaskSummary(info.execId).jvmGCTime
        }

        _executorData.setExecutorInfo(info.execId, info)
      }*/
    }
    _executorData
  }

  override def getJobProgressData(): SparkJobProgressData = {
    if (_jobProgressData == null) {
      _jobProgressData = new SparkJobProgressData()

      val jobInfoList = appStatusStore.jobsList(null)
      jobInfoList.foreach { case (data : JobData) =>
        val jobInfo = new JobInfo()

        val id = data.jobId;
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

        jobInfo.startTime = data.submissionTime.get.getTime
        jobInfo.endTime = data.completionTime.get.getTime

        data.stageIds.foreach{ case (id: Int) => jobInfo.addStageId(id)}

        val m = new mutable.HashSet[Int]()
        m.add(data.numCompletedStages)
        addIntSetToJSet(m, jobInfo.completedStageIndices)

        _jobProgressData.addJobInfo(id, jobInfo)

        val statV = data.status.name()
        if (statV.equals((JobExecutionStatus.FAILED.name())) || statV.eq((JobExecutionStatus.SUCCEEDED.name()))) {
          _jobProgressData.addCompletedJob(data.jobId)
        }
        if (statV.equals((JobExecutionStatus.FAILED.name()))) {
          _jobProgressData.addFailedJob(data.jobId)
        }

      }

      // Add Stage Info
      val stageInfos = appStatusStore.stageList(null)
      stageInfos.foreach{case (data) =>
        val stageInfo = new SparkJobProgressData.StageInfo()
        stageInfo.name = data.name

        stageInfo.description = data.description.getOrElse("")
        stageInfo.diskBytesSpilled = data.diskBytesSpilled
        stageInfo.executorRunTime = data.executorRunTime

        val ct = data.completionTime.getOrElse(null)
        val st = data.submissionTime.getOrElse(null)

        if (ct == null || st == null) {
          stageInfo.duration = 0 ;
        } else {
          stageInfo.duration = data.completionTime.get.getTime - data.submissionTime.get.getTime
        }

        stageInfo.inputBytes = data.inputBytes
        stageInfo.memoryBytesSpilled = data.memoryBytesSpilled
        stageInfo.numActiveTasks = data.numActiveTasks
        stageInfo.numCompleteTasks = data.numCompleteTasks
        stageInfo.numFailedTasks = data.numFailedTasks
        stageInfo.outputBytes = data.outputBytes
        stageInfo.shuffleReadBytes = data.shuffleReadBytes
        stageInfo.shuffleWriteBytes = data.shuffleWriteBytes

        val m = new mutable.HashSet[Int]()
        m.add(data.numCompletedIndices)
        addIntSetToJSet(m, stageInfo.completedIndices)

        _jobProgressData.addStageInfo(data.stageId,  data.attemptId, stageInfo)

        val statV = data.status.name()
//        println("statV=" + statV)

        if (statV.equals((StageStatus.FAILED.name())) || statV.eq((StageStatus.COMPLETE.name())) || statV.eq((StageStatus.SKIPPED.name()))) {
          _jobProgressData.addCompletedStages(data.stageId,  data.attemptId)
        }

        if (statV.equals((StageStatus.FAILED.name()))) {
          _jobProgressData.addFailedStages(data.stageId,  data.attemptId)
        }
      }

      // Add JobInfo
      /*jobProgressListener.jobIdToData.foreach { case (id, data) =>
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
      }*/
    }
    _jobProgressData
  }

  // This method returns a combined information from StorageStatusListener and StorageListener
  override def getStorageData(): SparkStorageData = {
    if (_storageData == null) {
      _storageData = new SparkStorageData()
      val rddInfoList = appStatusStore.rddList(true)

      _storageData.setRddStorageInfoList(toJList[RDDStorageInfo](rddInfoList))
      /*_storageData.setRddInfoList(toJList[RDDInfo](storageListener.rddInfoList))
      _storageData.setStorageStatusList(toJList[StorageStatus](storageStatusListener.storageStatusList))*/
    }
    _storageData
  }

  override def getAppId: String = {
    getGeneralData().getApplicationId
  }

  def load(in: InputStream, sourceName: String): Unit = {
    val replayBus = new ReplayListenerBus()



//    replayBus.addListener(applicationEventListener)
    /*replayBus.addListener(jobProgressListener)
    replayBus.addListener(environmentListener)
    replayBus.addListener(storageStatusListener)
    replayBus.addListener(executorsListener)
    replayBus.addListener(storageListener)
    replayBus.addListener(storageStatusTrackingListener)*/


    replayBus.addListener(appStatusStore.listener.get)
    replayBus.addListener(statsReportListener)

    replayBus.replay(in, sourceName, maybeTruncated = false)
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

  def main(args: Array[String]): Unit = {
    val gson = new Gson
    resource.managed {
      new ZipInputStream(new BufferedInputStream(new FileInputStream("E:\\eventLogs-application_7151955101801_11239909.zip")))
    }.acquireAndGet { zipInputStream =>
      getLogInputStream(zipInputStream) match {
        case (None, _) => throw new RuntimeException(s"Failed to read log for application ")
        case (Some(inputStream), fileName) => {
          val dataCollection = new SparkDataCollection()
          val fileName = "eventLogs-application_7151955101801_11239909.zip"
          dataCollection.load(inputStream, fileName)
          val sparkDataCollection = LegacyDataConverters.convert(dataCollection)
          // Augment missing fields, which would happen typically for backfill jobs.
//          augemntAnalyticJob(analyticJob, dataCollection, sparkDataCollection)
          println(sparkDataCollection.appId)

          println("_applicationData=" + gson.toJson(dataCollection._applicationData))
          println("_jobProgressData=" + gson.toJson(dataCollection._jobProgressData))
          println("_environmentData=" + gson.toJson(dataCollection._environmentData))
          println("_executorData=" + gson.toJson(dataCollection._executorData))
          println("_storageData=" + gson.toJson(dataCollection._storageData))
          println("_isThrottled=" + gson.toJson(dataCollection._isThrottled))
//          sparkDataCollection


          val data = sparkDataCollection

          println("type = " + gson.toJson(data.getApplicationType()));

          println("HadoopApplicationData=" + gson.toJson(data))
//          val jobType = ElephantContext.instance.matchJobType(data)
          val jobTypeName = data.getApplicationType().getName


          // Run all heuristics over the fetched data
          /*val analysisResults = new util.ArrayList[HeuristicResult]
          if (data == null || data.isEmpty) { // Example: a MR job has 0 mappers and 0 reducers
            println("No Data Received for analytic job")
            analysisResults.add(HeuristicResult.NO_DATA)
          }
          else {
            val heuristics = ElephantContext.instance.getHeuristicsForApplicationType(data.getApplicationType())
            println("getHeuristicsForApplicationType=" + gson.toJson(heuristics))
            import scala.collection.JavaConversions._
            for (heuristic <- heuristics) {
              println("heuristic.getHeuristicConfData=" + gson.toJson(heuristic.getHeuristicConfData))
              val confExcludedApps = heuristic.getHeuristicConfData.getParamMap.get("exclude_jobtypes_filter")
              println("confExcludedApps: " + confExcludedApps + "   cls:" + heuristic.getHeuristicConfData.getClassName)
              if (confExcludedApps == null || confExcludedApps.length == 0 || !Arrays.asList(confExcludedApps.split(",")).contains(jobTypeName)) {
//                val result = heuristic.apply(data)
//                if (result != null) analysisResults.add(result)
              }
            }
          }*/

          val res = new AppResult

          res
          res.save()

        }
      }
    }
  }

  private def getLogInputStream(zis: ZipInputStream): (Option[InputStream], String) = {
    // The logs are stored in a ZIP archive with a single entry.
    // It should be named as "$logPrefix.$archiveExtension", but
    // we trust Spark to get it right.
    val entry = zis.getNextEntry
    if (entry == null) {
//      println(s"failed to resolve log for ${attemptTarget.getUri}")
      (None, "")
    } else {
      val entryName = entry.getName
      if (entryName.endsWith(IN_PROGRESS)) {
        // Making sure that the application has finished.
        throw new RuntimeException(s"Application for the log ${entryName} has not finished yet.")
      }
      val codec = SparkUtils.compressionCodecForLogName(new SparkConf(), entryName)
      (Some(codec.map {
        _.compressedInputStream(zis)
      }.getOrElse(zis)), entryName)
    }
  }
}
