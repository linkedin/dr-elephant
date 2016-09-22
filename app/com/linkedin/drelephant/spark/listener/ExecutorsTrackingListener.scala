package com.linkedin.drelephant.spark.listener

import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark.scheduler._
import org.apache.spark.storage.{StorageStatus, StorageStatusListener}
import org.apache.spark.{ExceptionFailure, Resubmitted}

/**
  * A modified version of ExecutorsListener that tracks all existed executors during the entire application runtime.
  *
  * In YARN executor-dynamic-allocation mode, some executor will be killed when idle, and the other executor will
  * survive until end of the application.
  * Also, ExecutorsListener within Spark has much modification from 1.4 to 1.6 and 2.0, so build customized listener.
  * TODO:
  */
class ExecutorsTrackingListener(storageStatusListener: StorageStatusListener) extends SparkListener {
  val executorToTasksActive = HashMap[String, Int]()
  val executorToTasksComplete = HashMap[String, Int]()
  val executorToTasksFailed = HashMap[String, Int]()
  val executorToDuration = HashMap[String, Long]()
  val executorToInputBytes = HashMap[String, Long]()
  val executorToInputRecords = HashMap[String, Long]()
  val executorToOutputBytes = HashMap[String, Long]()
  val executorToOutputRecords = HashMap[String, Long]()
  val executorToShuffleRead = HashMap[String, Long]()
  val executorToShuffleWrite = HashMap[String, Long]()
  val executorToLogUrls = HashMap[String, Map[String, String]]()
  val executorIdToData = HashMap[String, ExecutorLifeData]()

  def storageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    executorToLogUrls(eid) = executorAdded.executorInfo.logUrlMap
    executorIdToData(eid) = ExecutorLifeData(executorAdded.time)
  }

  override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    val eid = executorRemoved.executorId
    val lifeData = executorIdToData(eid)
    lifeData.finishTime = Some(executorRemoved.time)
    lifeData.finishReason = Some(executorRemoved.reason)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 0) + 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId
      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure =>
          executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
        case _ =>
          executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
      }

      executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 1) - 1
      executorToDuration(eid) = executorToDuration.getOrElse(eid, 0L) + info.duration

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        metrics.inputMetrics.foreach { inputMetrics =>
          executorToInputBytes(eid) =
            executorToInputBytes.getOrElse(eid, 0L) + inputMetrics.bytesRead
          executorToInputRecords(eid) =
            executorToInputRecords.getOrElse(eid, 0L) + inputMetrics.recordsRead
        }
        metrics.outputMetrics.foreach { outputMetrics =>
          executorToOutputBytes(eid) =
            executorToOutputBytes.getOrElse(eid, 0L) + outputMetrics.bytesWritten
          executorToOutputRecords(eid) =
            executorToOutputRecords.getOrElse(eid, 0L) + outputMetrics.recordsWritten
        }
        metrics.shuffleReadMetrics.foreach { shuffleRead =>
          executorToShuffleRead(eid) =
            executorToShuffleRead.getOrElse(eid, 0L) + shuffleRead.remoteBytesRead
        }
        metrics.shuffleWriteMetrics.foreach { shuffleWrite =>
          executorToShuffleWrite(eid) =
            executorToShuffleWrite.getOrElse(eid, 0L) + shuffleWrite.shuffleBytesWritten
        }
      }
    }
  }

}

case class ExecutorLifeData(
                             val startTime: Long,
                             var finishTime: Option[Long] = None,
                             var finishReason: Option[String] = None)
