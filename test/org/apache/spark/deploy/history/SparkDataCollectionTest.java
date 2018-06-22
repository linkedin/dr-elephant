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

package org.apache.spark.deploy.history;

import com.linkedin.drelephant.spark.legacydata.SparkJobProgressData;
import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.ApplicationEventListener;
import org.apache.spark.scheduler.ReplayListenerBus;
import org.apache.spark.storage.StorageStatusListener;
import org.apache.spark.storage.StorageStatusTrackingListener;
import org.apache.spark.ui.env.EnvironmentListener;
import org.apache.spark.ui.exec.ExecutorsListener;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.apache.spark.ui.storage.StorageListener;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertNotNull;

public class SparkDataCollectionTest {

    private static final String event_log_dir = "spark_event_logs/";

    @Test
    public void testCollectJobProgressData() throws IOException {
        SparkDataCollection dataCollection = new SparkDataCollection();

        InputStream in = new BufferedInputStream(
                SparkDataCollectionTest.class.getClassLoader().getResourceAsStream(event_log_dir + "event_log_230"));
        dataCollection.load(in, in.toString());
        in.close();

        SparkJobProgressData jobProgressData = dataCollection.getJobProgressData();
        assertNotNull("can't get job progress data", jobProgressData);
    }

}
