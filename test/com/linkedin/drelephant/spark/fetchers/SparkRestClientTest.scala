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

package com.linkedin.drelephant.spark.fetchers

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, SimpleTimeZone}

import scala.concurrent.ExecutionContext
import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfo, ApplicationInfo, ExecutorSummary, JobData, StageData}
import javax.ws.rs.{GET, Path, PathParam, Produces}
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.{Application, MediaType}
import javax.ws.rs.ext.ContextResolver
import org.apache.spark.SparkConf
import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.{JerseyTest, TestProperties}
import org.scalatest.{AsyncFunSpec, Matchers}
import org.scalatest.compatible.Assertion

class SparkRestClientTest extends AsyncFunSpec with Matchers {
  import SparkRestClientTest._

  describe("SparkRestClient") {
    it("throws an exception if spark.eventLog.dir is missing") {
      an[IllegalArgumentException] should be thrownBy(new SparkRestClient(new SparkConf()))
    }

    it("returns the desired data from the Spark REST API for cluster mode application") {
      import ExecutionContext.Implicits.global
      val fakeJerseyServer = new FakeJerseyServer() {
        override def configure(): Application = super.configure() match {
          case resourceConfig: ResourceConfig =>
            resourceConfig
              .register(classOf[FetchClusterModeDataFixtures.ApiResource])
              .register(classOf[FetchClusterModeDataFixtures.ApplicationResource])
              .register(classOf[FetchClusterModeDataFixtures.JobsResource])
              .register(classOf[FetchClusterModeDataFixtures.StagesResource])
              .register(classOf[FetchClusterModeDataFixtures.ExecutorsResource])
          case config => config
        }
      }

      fakeJerseyServer.setUp()

      val historyServerUri = fakeJerseyServer.target.getUri

      val sparkConf = new SparkConf().set("spark.yarn.historyServer.address", s"${historyServerUri.getHost}:${historyServerUri.getPort}")
      val sparkRestClient = new SparkRestClient(sparkConf)

      sparkRestClient.fetchData(FetchClusterModeDataFixtures.APP_ID) map { restDerivedData =>
        restDerivedData.applicationInfo.id should be(FetchClusterModeDataFixtures.APP_ID)
        restDerivedData.applicationInfo.name should be(FetchClusterModeDataFixtures.APP_NAME)
        restDerivedData.jobDatas should not be(None)
        restDerivedData.stageDatas should not be(None)
        restDerivedData.executorSummaries should not be(None)
      } andThen { case assertion: Try[Assertion] =>
          fakeJerseyServer.tearDown()
          assertion
      }
    }

    it("returns the desired data from the Spark REST API for client mode application") {
      import ExecutionContext.Implicits.global
      val fakeJerseyServer = new FakeJerseyServer() {
        override def configure(): Application = super.configure() match {
          case resourceConfig: ResourceConfig =>
            resourceConfig
              .register(classOf[FetchClientModeDataFixtures.ApiResource])
              .register(classOf[FetchClientModeDataFixtures.ApplicationResource])
              .register(classOf[FetchClientModeDataFixtures.JobsResource])
              .register(classOf[FetchClientModeDataFixtures.StagesResource])
              .register(classOf[FetchClientModeDataFixtures.ExecutorsResource])
          case config => config
        }
      }

      fakeJerseyServer.setUp()

      val historyServerUri = fakeJerseyServer.target.getUri

      val sparkConf = new SparkConf().set("spark.yarn.historyServer.address", s"${historyServerUri.getHost}:${historyServerUri.getPort}")
      val sparkRestClient = new SparkRestClient(sparkConf)

      sparkRestClient.fetchData(FetchClusterModeDataFixtures.APP_ID) map { restDerivedData =>
        restDerivedData.applicationInfo.id should be(FetchClusterModeDataFixtures.APP_ID)
        restDerivedData.applicationInfo.name should be(FetchClusterModeDataFixtures.APP_NAME)
        restDerivedData.jobDatas should not be(None)
        restDerivedData.stageDatas should not be(None)
        restDerivedData.executorSummaries should not be(None)
      } andThen { case assertion: Try[Assertion] =>
        fakeJerseyServer.tearDown()
        assertion
      }
    }
  }
}

object SparkRestClientTest {
  class FakeJerseyServer extends JerseyTest {
    override def configure(): Application = {
      forceSet(TestProperties.CONTAINER_PORT, "0")
      enable(TestProperties.LOG_TRAFFIC)
      enable(TestProperties.DUMP_ENTITY)

      new ResourceConfig()
        .register(classOf[FakeJerseyObjectMapperProvider])
    }

    override def configureClient(clientConfig: ClientConfig): Unit = {
      clientConfig.register(classOf[FakeJerseyObjectMapperProvider])
    }
  }

  class FakeJerseyObjectMapperProvider extends ContextResolver[ObjectMapper] {
    lazy val objectMapper = {
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.setDateFormat(dateFormat)
      objectMapper
    }

    lazy val dateFormat = {
      val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
      val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
      iso8601.setCalendar(cal)
      iso8601
    }

    override def getContext(cls: Class[_]): ObjectMapper = objectMapper
  }

  object FetchClusterModeDataFixtures {
    val APP_ID = "application_1"
    val APP_NAME = "app"

    @Path("/api/v1")
    class ApiResource {
      @Path("applications/{appId}")
      def getApplication(): ApplicationResource = new ApplicationResource()

      @Path("applications/{appId}/{attemptId}/jobs")
      def getJobs(): JobsResource = new JobsResource()

      @Path("applications/{appId}/{attemptId}/stages")
      def getStages(): StagesResource = new StagesResource()

      @Path("applications/{appId}/{attemptId}/executors")
      def getExecutors(): ExecutorsResource = new ExecutorsResource()
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class ApplicationResource {
      @GET
      def getApplication(@PathParam("appId") appId: String): ApplicationInfo = {
        val t2 = System.currentTimeMillis
        val t1 = t2 - 1
        val duration = 8000000L
        new ApplicationInfo(
          APP_ID,
          APP_NAME,
          Seq(
            newFakeApplicationAttemptInfo(Some("2"), startTime = new Date(t2 - duration), endTime = new Date(t2)),
            newFakeApplicationAttemptInfo(Some("1"), startTime = new Date(t1 - duration), endTime = new Date(t1))
          )
        )
      }
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class JobsResource {
      @GET
      def getJobs(@PathParam("appId") appId: String, @PathParam("attemptId") attemptId: String): Seq[JobData] =
        if (attemptId == "2") Seq.empty else throw new Exception()
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class StagesResource {
      @GET
      def getStages(@PathParam("appId") appId: String, @PathParam("attemptId") attemptId: String): Seq[StageData] =
        if (attemptId == "2") Seq.empty else throw new Exception()
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class ExecutorsResource {
      @GET
      def getExecutors(@PathParam("appId") appId: String, @PathParam("attemptId") attemptId: String): Seq[ExecutorSummary] =
        if (attemptId == "2") Seq.empty else throw new Exception()
    }
  }

  object FetchClientModeDataFixtures {
    val APP_ID = "application_1"
    val APP_NAME = "app"

    @Path("/api/v1")
    class ApiResource {
      @Path("applications/{appId}")
      def getApplication(): ApplicationResource = new ApplicationResource()

      @Path("applications/{appId}/jobs")
      def getJobs(): JobsResource = new JobsResource()

      @Path("applications/{appId}/stages")
      def getStages(): StagesResource = new StagesResource()

      @Path("applications/{appId}/executors")
      def getExecutors(): ExecutorsResource = new ExecutorsResource()
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class ApplicationResource {
      @GET
      def getApplication(@PathParam("appId") appId: String): ApplicationInfo = {
        val t2 = System.currentTimeMillis
        val t1 = t2 - 1
        val duration = 8000000L
        new ApplicationInfo(
          APP_ID,
          APP_NAME,
          Seq(
            newFakeApplicationAttemptInfo(None, startTime = new Date(t2 - duration), endTime = new Date(t2)),
            newFakeApplicationAttemptInfo(None, startTime = new Date(t1 - duration), endTime = new Date(t1))
          )
        )
      }
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class JobsResource {
      @GET
      def getJobs(@PathParam("appId") appId: String): Seq[JobData] =
        Seq.empty
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class StagesResource {
      @GET
      def getStages(@PathParam("appId") appId: String): Seq[StageData] =
        Seq.empty
    }

    @Produces(Array(MediaType.APPLICATION_JSON))
    class ExecutorsResource {
      @GET
      def getExecutors(@PathParam("appId") appId: String): Seq[ExecutorSummary] =
        Seq.empty
    }
  }

  def newFakeApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Date,
    endTime: Date
  ): ApplicationAttemptInfo = new ApplicationAttemptInfo(
    attemptId,
    startTime,
    endTime,
    sparkUser = "foo",
    completed = true
  )
}
