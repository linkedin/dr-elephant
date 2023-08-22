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

package com.linkedin.drelephant.priorityexecutor;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test Priority based thread pool executor.
 */
public class PriorityBasedThreadPoolExecutorTest {

  @Test (timeout = 120000)
  public void testPriorityBasedExecutor() throws Exception {
    List<Priority> priorityList = Collections.synchronizedList(new ArrayList<Priority>());
    ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("priority-exec-thread-%d").build();
    ThreadPoolExecutor threadPoolExecutor = new PriorityBasedThreadPoolExecutor(2, 2, 0L,
        TimeUnit.MILLISECONDS, factory);
    Future<?> future1 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.LOW), Priority.LOW));
    Future<?> future2 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.LOW), Priority.LOW));
    Future<?> future3 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.LOW), Priority.LOW));
    Future<?> future4 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.LOW), Priority.LOW));
    Future<?> future5 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.NORMAL), Priority.NORMAL));
    Future<?> future6 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.NORMAL), Priority.NORMAL));
    Future<?> future7 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.HIGH), Priority.HIGH));
    Future<?> future8 = threadPoolExecutor.submit(
        RunnableWithPriority.get(new DummyRunnable(priorityList, Priority.HIGH), Priority.HIGH));
    List<Priority> expectedPriorityList = Lists.newArrayList(Priority.LOW, Priority.LOW);
    while (priorityList.size() < 2) {
      Thread.sleep(100);
    }
    Assert.assertEquals(expectedPriorityList, priorityList);
    future1.cancel(true);
    future2.cancel(true);
    expectedPriorityList.addAll(Lists.newArrayList(Priority.HIGH, Priority.HIGH));
    while (priorityList.size() < 4) {
      Thread.sleep(100);
    }
    Assert.assertEquals(expectedPriorityList, priorityList);
    future7.cancel(true);
    future8.cancel(true);
    expectedPriorityList.addAll(Lists.newArrayList(Priority.NORMAL, Priority.NORMAL));
    while (priorityList.size() < 6) {
      Thread.sleep(100);
    }
    Assert.assertEquals(expectedPriorityList, priorityList);
    future5.cancel(true);
    future6.cancel(true);
    expectedPriorityList.addAll(Lists.newArrayList(Priority.LOW, Priority.LOW));
    while (priorityList.size() < 8) {
      Thread.sleep(100);
    }
    Assert.assertEquals(expectedPriorityList, priorityList);
    future3.cancel(true);
    future4.cancel(true);
    threadPoolExecutor.shutdownNow();
  }

  private static class DummyRunnable implements Runnable {
    private List<Priority> _priorityList;
    private Priority _priority;
    private DummyRunnable(List<Priority> priorityList, Priority priority) {
      _priorityList = priorityList;
      _priority = priority;
    }

    @Override
    public void run() {
      _priorityList.add(_priority);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {}
    }
  }
}
