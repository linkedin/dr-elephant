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

package com.linkedin.drelephant.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.NotInMountpointException;
import org.apache.log4j.Logger;
import java.io.IOException;


/**
 * The HDFS Information
 */
public final class HDFSContext {
  private static final Logger logger = Logger.getLogger(HDFSContext.class);

  public static long HDFS_BLOCK_SIZE = 64 * 1024 * 1024;
  public static final long DISK_READ_SPEED = 100 * 1024 * 1024;

  private HDFSContext() {
    // Empty on purpose
  }

  /**
   * Captures the HDFS Block Size
   */
  public static void load() {
    try {
      HDFS_BLOCK_SIZE = FileSystem.get(new Configuration()).getDefaultBlockSize(new Path("/"));
    } catch (IOException e) {
      logger.error("Error getting FS Block Size!", e);
    } catch (NotInMountpointException e) {
      logger.warn("Fix hdfs federation : The path / is not exist. Will try /tmp");
      try {
        HDFS_BLOCK_SIZE = FileSystem.get(new Configuration()).getDefaultBlockSize(new Path("/tmp"));
      } catch (IOException e1) {
        e1.printStackTrace();
      } catch (NotInMountpointException e2){
        logger.warn("Fix hdfs federation : The path /tmp is not exist. Use default block size 128 * 1024 * 1024");
        /**
         * Currently most companies use 128M as the default block size
         */
        HDFS_BLOCK_SIZE = 128 * 1024 * 1024;
      }
    } catch (Exception s){
      logger.warn("Error getting FS Block Size!", s);
      HDFS_BLOCK_SIZE = 128 * 1024 * 1024;
    }

    logger.info("HDFS BLock size: " + HDFS_BLOCK_SIZE);
  }
}
