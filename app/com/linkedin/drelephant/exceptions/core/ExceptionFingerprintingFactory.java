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

package com.linkedin.drelephant.exceptions.core;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.ExceptionFingerprinting;
import com.linkedin.drelephant.spark.data.SparkApplicationData;

import static com.linkedin.drelephant.exceptions.util.Constant.*;

import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import org.apache.log4j.Logger;
import scala.collection.Seq;
import scala.collection.convert.WrapAsJava$;


/**
 * Factory class to produce ExceptionFingerprinting object based on execution engine type.
 */
public class ExceptionFingerprintingFactory {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingFactory.class);

  public static ExceptionFingerprinting getExceptionFingerprinting(ExecutionEngineType executionEngineType,
      HadoopApplicationData data) {
    switch (executionEngineType) {
      case SPARK:
        logger.info(" Spark Exception Fingerprinting is called ");
        Seq<StageData> stagesWithFailedTasks = ((SparkApplicationData) data).stagesWithFailedTasks();
        if (stagesWithFailedTasks != null) {
          logger.info(" Size of stages with failed task " + stagesWithFailedTasks.size());
          return new ExceptionFingerprintingSpark(WrapAsJava$.MODULE$.seqAsJavaList(stagesWithFailedTasks));
        } else {
          return new ExceptionFingerprintingSpark(null);
        }
        //TODO : Create MR exception fingerprinting for Map Reduce jobs
      case MR:
        logger.info(" MR Exception Fingerprinting  is called ");
        return null;
      default:
        logger.error(" Unknown execution engine type ");
    }
    return null;
  }
}
