package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class TuningHelper {
  private static final Logger logger = Logger.getLogger(TuningHelper.class);
  private static boolean debugEnabled = logger.isDebugEnabled();
  private static final int ONE_GB = 1024;
  private static final int MINIMUM_HEAP_SIZE_MB = 600;
  private static final String PATTERN_FOR_MEMORY = "(^|\\s)-Xmx[0-9]+[GgMmBbKk]($|\\s)";
  private static final String PATTERN_FOR_TIME = "[0-9]+[hr|min|sec]+";
  private static final String MAX_HEAP_SIZE_KEYWORD="-Xmx";
  private static final int DEFAULT_MAX_HEAP_SIZE = 1536;

  private enum TimeUnit {hr, min, sec}

  public static List<TuningParameter> getTuningParameterList(TuningJobDefinition tuningJobDefinition) {
    List<TuningParameter> tuningParameterList = TuningParameter.find.where()
        .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id,
            tuningJobDefinition.tuningAlgorithm.id)
        .eq(TuningParameter.TABLE.isDerived, 0)
        .findList();
    return tuningParameterList;
  }

  public static JobSuggestedParamSet getDefaultParameterValuesforJob(TuningJobDefinition tuningJobDefinition) {
    JobSuggestedParamSet defaultJobParamSet = JobSuggestedParamSet.find.where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, tuningJobDefinition.job.id)
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 1)
        .order()
        .desc(JobSuggestedParamSet.TABLE.id)
        .setMaxRows(1)
        .findUnique();
    return defaultJobParamSet;
  }

  public static TuningJobDefinition getTuningJobDefinition(JobDefinition job) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .findUnique();
    return tuningJobDefinition;
  }

  public static List<TuningParameter> getDerivedParameterList(TuningJobDefinition tuningJobDefinition) {
    List<TuningParameter> derivedParameterList = new ArrayList<TuningParameter>();
    derivedParameterList = TuningParameter.find.where()
        .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id,
            tuningJobDefinition.tuningAlgorithm.id)
        .eq(TuningParameter.TABLE.isDerived, 1)
        .findList();
    return derivedParameterList;
  }

  public static List<TuningJobExecutionParamSet> getTuningJobExecutionFromDefinition(JobDefinition jobDefinition) {
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
        TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
            .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
            .where()
            .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.jobDefinition
                + '.' + JobDefinition.TABLE.id, jobDefinition.id)
            .order()
            .desc("job_execution_id")
            .findList();
    return tuningJobExecutionParamSets;
  }

  public static int getNumberOfValidSuggestedParamExecution(
      List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    int autoAppliedExecution = 0;
    for (TuningJobExecutionParamSet tuningJobExecutionParam : tuningJobExecutionParamSets) {
      if (tuningJobExecutionParam.jobSuggestedParamSet.isParamSetSuggested
          && !tuningJobExecutionParam.jobSuggestedParamSet.paramSetState.equals(
          JobSuggestedParamSet.ParamSetStatus.DISCARDED)) {
        autoAppliedExecution++;
      }
    }
    if (debugEnabled) {
      logger.debug(" Total number of executions after auto applied enabled " + autoAppliedExecution);
    }
    return autoAppliedExecution;
  }

  public static boolean isNewParamBestParam(JobSuggestedParamSet jobSuggestedParamSet,
      JobSuggestedParamSet currentBestJobSuggestedParamSet) {
    boolean newParamBestParam = false;
    if (currentBestJobSuggestedParamSet.fitnessJobExecution.inputSizeInBytes > 1
        && jobSuggestedParamSet.fitnessJobExecution.inputSizeInBytes > 1) {
      Double currentBestResourceUsagePerGBInput =
          currentBestJobSuggestedParamSet.fitnessJobExecution.resourceUsage * FileUtils.ONE_GB
              / currentBestJobSuggestedParamSet.fitnessJobExecution.inputSizeInBytes;
      Double newResourceUsagePerGBInput = jobSuggestedParamSet.fitnessJobExecution.resourceUsage * FileUtils.ONE_GB
          / jobSuggestedParamSet.fitnessJobExecution.inputSizeInBytes;
      if (newResourceUsagePerGBInput < currentBestResourceUsagePerGBInput) {
        newParamBestParam = true;
      }
    } else {
      if (currentBestJobSuggestedParamSet.fitnessJobExecution.resourceUsage
          > jobSuggestedParamSet.fitnessJobExecution.resourceUsage) {
        newParamBestParam = true;
      }
    }
    return newParamBestParam;
  }

  public static void updateJobSuggestedParamSet(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution) {
    try {
      jobSuggestedParamSet.update();
    } catch (Exception e) {
      logger.error("Exception updating job suggested param set " + jobSuggestedParamSet.id + " "
          + jobSuggestedParamSet.fitnessJobExecution.id + " " + jobExecution.id, e);
    }
  }

  public static void updateJobExecution(JobExecution jobExecution) {
    try {
      jobExecution.update();
    } catch (Exception e) {
      logger.error("Exception updating job execution " + jobExecution.id, e);
    }
  }

  public static Double getContainerSize(Double memory) {
    return Math.ceil(memory / ONE_GB) * ONE_GB;
  }

  public static Double getHeapSize(Double heapSize) {
    if (heapSize < MINIMUM_HEAP_SIZE_MB) {
      heapSize = MINIMUM_HEAP_SIZE_MB * 1.0;
    }
    return heapSize;
  }

  public static Double parseMaxHeapSizeInMB(String data) {
    try {
      logger.debug(" Heap data " + data);
      Pattern pattern = Pattern.compile(PATTERN_FOR_MEMORY);
      Matcher m = pattern.matcher(data);
      String maxHeapSize;
      if (m.find()) {
        maxHeapSize = m.group().trim();
        maxHeapSize = maxHeapSize.substring(MAX_HEAP_SIZE_KEYWORD.length());
        return MemoryFormatUtils.stringToBytes(maxHeapSize) / (1024.0 * 1024.0);
      }
    } catch (Exception e) {
      logger.error(" Error gettting max heap ", e);
    }

    logger.info(" Max heap size not found ");
    return DEFAULT_MAX_HEAP_SIZE * 1.0;
  }

  public static double getTimeInMinute(String value) {
    value = value.replaceAll(" ", "");
    Pattern pattern = Pattern.compile(PATTERN_FOR_TIME);
    Matcher m = pattern.matcher(value);
    double timeInMinutes = 0.0;
    try {
      while (m.find()) {
        String s = m.group().trim();
        if (s.contains(TimeUnit.hr.name())) {
          timeInMinutes =
              timeInMinutes + Double.parseDouble(s.substring(0, s.length() - TimeUnit.hr.name().length())) * 60;
        } else if (s.contains(TimeUnit.min.name())) {
          timeInMinutes = timeInMinutes + Double.parseDouble(s.substring(0, s.length() - TimeUnit.min.name().length()));
        } else if (s.contains(TimeUnit.sec.name())) {
          timeInMinutes =
              timeInMinutes + Double.parseDouble(s.substring(0, s.length() - TimeUnit.sec.name().length())) / 60.0;
        }
      }
    } catch (NumberFormatException exception) {
      logger.error(" Error while parsing data in time ");
    }
    return timeInMinutes;
  }

  static JobSuggestedParamSet getLatestCreatedJobSuggestedParamSet(
      long jobDefinitionId) {
    return JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id,
            jobDefinitionId)
        .order()
        .desc(JobSuggestedParamSet.TABLE.updatedTs)
        .setMaxRows(1)
        .findUnique();
  }

  static JobSuggestedParamSet getDefaultParamSet(long jobDefinitionId) {
    return JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id,
            jobDefinitionId)
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, true)
        .ne(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus
            .DISCARDED)
        .findUnique();
  }
}
