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

package controllers;

import com.avaje.ebean.Query;

import models.AppResult;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.GlobalSettings;
import play.api.mvc.Content;
import play.test.FakeApplication;
import views.html.page.homePage;
import views.html.results.searchResults;

import java.util.HashMap;
import java.util.Map;

import static common.TestConstants.*;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_KEY;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;


public class ApplicationTest {

  private static final Logger logger = LoggerFactory.getLogger(ApplicationTest.class);

  @Test
  public void testRenderHomePage() {
    Content html = homePage.render(5, 2, 3, searchResults.render("Latest analysis", null));
    assertEquals("text/html", html.contentType());
    assertTrue(html.body().contains("Hello there, I've been busy!"));
    assertTrue(html.body().contains("I looked through <b>5</b> jobs today."));
    assertTrue(html.body().contains("About <b>2</b> of them could use some tuning."));
    assertTrue(html.body().contains("About <b>3</b> of them need some serious attention!"));
  }

  @Test
  public void testRenderSearch() {
    Content html = searchResults.render("Latest analysis", null);
    assertEquals("text/html", html.contentType());
    assertTrue(html.body().contains("Latest analysis"));
  }

  private static FakeApplication fakeApp;

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<String, String>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(play.Application app) {
        logger.info("Starting FakeApplication");
      }
    };

    fakeApp = fakeApplication(dbConn, gs);
  }

  @Test
  public void testGenerateSearchQuery() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        Map<String, String> searchParams = new HashMap<String, String>();

        // Null searchParams Check
        Query<AppResult> query1 = Application.generateSearchQuery("*", null);
        assertNotNull(query1.findList());
        String sql1 = query1.getGeneratedSql();
        assertTrue(sql1.contains("select t0.id c0"));
        assertTrue(sql1.contains("from yarn_app_result t0 order by t0.finish_time desc"));

        // No searchParams Check
        Query<AppResult> query2 = Application.generateSearchQuery("*", searchParams);
        assertNotNull(query2.findList());
        String sql2 = query2.getGeneratedSql();
        assertTrue(sql2.contains("select t0.id c0"));
        assertTrue(sql2.contains("from yarn_app_result t0 order by t0.finish_time desc"));

        // Query by username
        searchParams.put(Application.USERNAME, "username");
        query2 = Application.generateSearchQuery("*", searchParams);
        assertNotNull(query2.findList());
        sql2 = query2.getGeneratedSql();
        assertTrue(sql2.contains("select t0.id c0"));
        assertTrue(sql2.contains("from yarn_app_result t0 where"));
        assertTrue(sql2.contains("t0.username = ?  order by t0.finish_time desc"));

        // Query by jobtype
        searchParams.put(Application.JOB_TYPE, "Pig");
        query2 = Application.generateSearchQuery("*", searchParams);
        assertNotNull(query2.findList());
        sql2 = query2.getGeneratedSql();
        assertTrue(sql2.contains("select t0.id c0"));
        assertTrue(sql2.contains("from yarn_app_result t0 where"));
        assertTrue(sql2.contains("t0.username = ?"));
        assertTrue(sql2.contains("t0.job_type = ?"));
        assertTrue(sql2.contains("order by t0.finish_time desc"));

        // Query by username, jobtype and start time
        searchParams.put(Application.STARTED_TIME_BEGIN, "1459713751000");
        searchParams.put(Application.STARTED_TIME_END, "1459713751000");
        Query<AppResult> query3 = Application.generateSearchQuery("*", searchParams);
        assertNotNull(query3.findList());
        String sql3 = query3.getGeneratedSql();
        assertTrue(sql3.contains("select t0.id c0"));
        assertTrue(sql3.contains("from yarn_app_result t0 where"));
        assertTrue(sql3.contains("t0.username = ?"));
        assertTrue(sql3.contains("t0.start_time >= ?"));
        assertTrue(sql3.contains("t0.start_time <= ?"));
        assertTrue(sql3.contains("order by t0.start_time desc"));

        // Query by finish time
        searchParams = new HashMap<String, String>();
        searchParams.put(Application.FINISHED_TIME_BEGIN, "1459713751000");
        searchParams.put(Application.FINISHED_TIME_END, "1459713751000");
        Query<AppResult> query4 = Application.generateSearchQuery("*", searchParams);
        assertNotNull(query4.findList());
        String sql4 = query4.getGeneratedSql();
        assertTrue(sql4.contains("select t0.id c0"));
        assertTrue(sql4.contains("from yarn_app_result t0 where"));
        assertTrue(sql4.contains("t0.finish_time >= ?"));
        assertTrue(sql4.contains("t0.finish_time <= ?"));
        assertTrue(sql4.contains("order by t0.finish_time desc"));
      }
    });
  }
}
