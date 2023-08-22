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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * Unit tests for class {@link AnalyticJobGeneratorHadoop2}.
 */
public class AnalyticJobGeneratorHadoop2Test {
  /**
   * Tests concurrent operations (fetch and add) on second retry queue.
   */
  @Test
  public void testSecondRetryQueueConcurrentOperations() {
    final AnalyticJobGeneratorHadoop2 analyticJobGenerator =
        new AnalyticJobGeneratorHadoop2();

    // Latch to ensure operations on second retry queue from multiple threads
    // run in parallel
    final CountDownLatch latch = new CountDownLatch(1);

    // Add a job into second retry queue.
    AnalyticJob job1 = spy(new AnalyticJob());
    // Custom answer on call to readyForSecondRetry for this job.
    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(final InvocationOnMock invocation) throws Throwable {
        // Wait for additions to second retry queue from add jobs thread to
        // begin. In case of synchronized access, wait will be for 5 sec.
        // In case of unsynchronized access, this would lead to
        // ConcurrentModificationException.
        latch.await(5000L, TimeUnit.MILLISECONDS);
        return true ;
      }
    }).when(job1).readyForSecondRetry();
    analyticJobGenerator.addIntoSecondRetryQueue(job1);

    // Add couple of other jobs to second retry queue.
    AnalyticJob job2 = spy(new AnalyticJob());
    when(job2.readyForSecondRetry()).thenReturn(false);
    analyticJobGenerator.addIntoSecondRetryQueue(job2);

    AnalyticJob job3 = spy(new AnalyticJob());
    when(job3.readyForSecondRetry()).thenReturn(true);
    analyticJobGenerator.addIntoSecondRetryQueue(job3);

    final List<AnalyticJob> appList = new ArrayList<AnalyticJob>();
    // Flag to indicate if ConcurrentModificationException has been thrown.
    final AtomicBoolean cmExceptionFlag = new AtomicBoolean(false);
    // Start a fetch jobs thread which calls fetchJobsFromSecondRetryQueue
    // method.
    Thread fetchJobsThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          analyticJobGenerator.fetchJobsFromSecondRetryQueue(appList);
        } catch (ConcurrentModificationException e) {
          cmExceptionFlag.set(true);
        }
      }
    });
    fetchJobsThread.start();

    // Start a add jobs jobs thread which adds a couple of jobs into second
    // retry queue while fetch jobs thread is running in parallel.
    Thread addJobsThread = new Thread(new Runnable() {
      @Override
      public void run() {
        AnalyticJob job4 = spy(new AnalyticJob());
        when(job4.readyForSecondRetry()).thenReturn(false);
        analyticJobGenerator.addIntoSecondRetryQueue(job4);

        // Latch countdown to ensure fetch jobs thread can continue.
        latch.countDown();

        AnalyticJob job5 = spy(new AnalyticJob());
        when(job5.readyForSecondRetry()).thenReturn(true);
        analyticJobGenerator.addIntoSecondRetryQueue(job5);
      }
    });
    addJobsThread.start();

    // Wait for both the threads to finish.
    try {
      fetchJobsThread.join();
      addJobsThread.join();
    } catch (InterruptedException e) {
      // Ignore the exception.
    }

    // Concurrent operations from multiple threads should not lead to
    // ConcurrentModificationException as accesses to second retry queue are
    // synchronized.
    assertFalse("ConcurrentModificationException should not have been thrown " +
        "while fetching jobs", cmExceptionFlag.get());
    // Checking for apps >= 2 as the exact number can be 2 or 3 depending on
    // order of invocation of threads.
    assertTrue("Apps fetched from second retry queue should be >= 2.",
        appList.size() >= 2);

    // Drain the second retry queue by fetching jobs from it.
    analyticJobGenerator.fetchJobsFromSecondRetryQueue(appList);
    assertEquals("Apps fetched from second retry queue should be 3.", 3,
        appList.size());
  }
}
