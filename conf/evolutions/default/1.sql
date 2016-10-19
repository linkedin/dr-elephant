#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# --- !Ups


CREATE TABLE yarn_app_result (
  id               VARCHAR(50)   NOT NULL              COMMENT 'The application id, e.g., application_1236543456321_1234567',
  name             VARCHAR(100)  NOT NULL              COMMENT 'The application name',
  username         VARCHAR(50)   NOT NULL              COMMENT 'The user who started the application',
  queue_name       VARCHAR(50)   DEFAULT NULL          COMMENT 'The queue the application was submitted to',
  start_time       BIGINT        UNSIGNED NOT NULL     COMMENT 'The time in which application started',
  finish_time      BIGINT        UNSIGNED NOT NULL     COMMENT 'The time in which application finished',
  tracking_url     VARCHAR(255)  NOT NULL              COMMENT 'The web URL that can be used to track the application',
  job_type         VARCHAR(20)   NOT NULL              COMMENT 'The Job Type e.g, Pig, Hive, Spark, HadoopJava',
  severity         TINYINT(2)    UNSIGNED NOT NULL     COMMENT 'Aggregate severity of all the heuristics. Ranges from 0(LOW) to 4(CRITICAL)',
  score            MEDIUMINT(9)  UNSIGNED DEFAULT 0    COMMENT 'The application score which is the sum of heuristic scores',
  workflow_depth   TINYINT(2)    UNSIGNED DEFAULT 0    COMMENT 'The application depth in the scheduled flow. Depth starts from 0',
  scheduler        VARCHAR(20)   DEFAULT NULL          COMMENT 'The scheduler which triggered the application',
  job_name         VARCHAR(255)  NOT NULL DEFAULT ''   COMMENT 'The name of the job in the flow to which this app belongs',
  job_exec_id      VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A unique reference to a specific execution of the job/action(job in the workflow). This should filter all applications (mapreduce/spark) triggered by the job for a particular execution.',
  flow_exec_id     VARCHAR(255)  NOT NULL DEFAULT ''   COMMENT 'A unique reference to a specific flow execution. This should filter all applications fired by a particular flow execution. Note that if the scheduler supports sub-workflows, then this ID should be the super parent flow execution id that triggered the the applications and sub-workflows.',
  job_def_id       VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A unique reference to the job in the entire flow independent of the execution. This should filter all the applications(mapreduce/spark) triggered by the job for all the historic executions of that job.',
  flow_def_id      VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A unique reference to the entire flow independent of any execution. This should filter all the historic mr jobs belonging to the flow. Note that if your scheduler supports sub-workflows, then this ID should reference the super parent flow that triggered the all the jobs and sub-workflows.',
  job_exec_url     VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A url to the job execution on the scheduler',
  flow_exec_url    VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A url to the flow execution on the scheduler',
  job_def_url      VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A url to the job definition on the scheduler',
  flow_def_url     VARCHAR(800)  NOT NULL DEFAULT ''   COMMENT 'A url to the flow definition on the scheduler',

  PRIMARY KEY (id)
);

create index yarn_app_result_i1 on yarn_app_result (finish_time);
create index yarn_app_result_i2 on yarn_app_result (username,finish_time);
create index yarn_app_result_i3 on yarn_app_result (job_type,username,finish_time);
create index yarn_app_result_i4 on yarn_app_result (flow_exec_id);
create index yarn_app_result_i5 on yarn_app_result (job_def_id);
create index yarn_app_result_i6 on yarn_app_result (flow_def_id);
create index yarn_app_result_i7 on yarn_app_result (start_time);

CREATE TABLE yarn_app_heuristic_result (
  id                  INT(11)       NOT NULL AUTO_INCREMENT COMMENT 'The application heuristic result id',
  yarn_app_result_id  VARCHAR(50)   NOT NULL                COMMENT 'The application id',
  heuristic_class     VARCHAR(255)  NOT NULL                COMMENT 'Name of the JVM class that implements this heuristic',
  heuristic_name      VARCHAR(128)  NOT NULL                COMMENT 'The heuristic name',
  severity            TINYINT(2)    UNSIGNED NOT NULL       COMMENT 'The heuristic severity ranging from 0(LOW) to 4(CRITICAL)',
  score               MEDIUMINT(9)  UNSIGNED DEFAULT 0      COMMENT 'The heuristic score for the application. score = severity * number_of_tasks(map/reduce) where severity not in [0,1], otherwise score = 0',

  PRIMARY KEY (id),
  CONSTRAINT yarn_app_heuristic_result_f1 FOREIGN KEY (yarn_app_result_id) REFERENCES yarn_app_result (id)
);

create index yarn_app_heuristic_result_i1 on yarn_app_heuristic_result (yarn_app_result_id);
create index yarn_app_heuristic_result_i2 on yarn_app_heuristic_result (heuristic_name,severity);

CREATE TABLE yarn_app_heuristic_result_details (
  yarn_app_heuristic_result_id  INT(11) NOT NULL                  COMMENT 'The application heuristic result id',
  name                          VARCHAR(128) NOT NULL DEFAULT ''  COMMENT 'The analysis detail entry name/key',
  value                         VARCHAR(255) NOT NULL DEFAULT ''  COMMENT 'The analysis detail value corresponding to the name',
  details                       TEXT                              COMMENT 'More information on analysis details. e.g, stacktrace',

  PRIMARY KEY (yarn_app_heuristic_result_id,name),
  CONSTRAINT yarn_app_heuristic_result_details_f1 FOREIGN KEY (yarn_app_heuristic_result_id) REFERENCES yarn_app_heuristic_result (id)
);

create index yarn_app_heuristic_result_details_i1 on yarn_app_heuristic_result_details (name);

# --- !Downs

SET FOREIGN_KEY_CHECKS=0;

DROP TABLE yarn_app_result;

DROP TABLE yarn_app_heuristic_result;

DROP TABLE yarn_app_heuristic_result_details;

SET FOREIGN_KEY_CHECKS=1;

# --- Created by Ebean DDL
# To stop Ebean DDL generation, remove this comment and start using Evolutions
