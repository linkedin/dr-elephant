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

/**
 * These queries are to make the database compliant for Unified Architecture 
 */

/**
 * Added created timestamp and updated time stamp in flow_definition,flow_execution,tuning_job_execution
 */
ALTER TABLE flow_definition ADD COLUMN created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE flow_definition ADD COLUMN updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE flow_execution ADD COLUMN created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE flow_execution ADD COLUMN updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE tuning_job_execution ADD COLUMN created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE tuning_job_execution ADD COLUMN updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

/**
 * Database schema changes to have more information about jobs and handle all possible scenarios
 */
ALTER TABLE tuning_job_execution DROP FOREIGN KEY tuning_job_execution_ibfk_2;
ALTER TABLE tuning_job_execution RENAME to job_suggested_param_set;
ALTER TABLE job_suggested_param_set ADD COLUMN are_constraints_violated tinyint(4) default 0 NOT NULL after param_set_state;
ALTER TABLE job_suggested_param_set CHANGE is_default_execution is_param_set_default tinyint(4) default 0 NOT NULL;
ALTER TABLE job_suggested_param_set ADD COLUMN id int(10) DEFAULT 0 NOT NULL after job_execution_id;
UPDATE job_suggested_param_set set id = job_execution_id;
ALTER TABLE job_suggested_param_set ADD PRIMARY KEY(id);
ALTER TABLE job_suggested_param_set ADD COLUMN job_definition_id int(10) unsigned NOT NULL after id ;
ALTER TABLE job_suggested_param_set CHANGE job_execution_id fitness_job_execution_id int(10) unsigned NULL;
ALTER TABLE job_suggested_param_value DROP FOREIGN KEY job_suggested_param_values_f1;
ALTER TABLE job_suggested_param_set CHANGE id id int(10) unsigned NOT NULL AUTO_INCREMENT;
UPDATE job_suggested_param_set a INNER JOIN job_execution b on a.fitness_job_execution_id = b.id set a.job_definition_id = b.job_definition_id;
ALTER TABLE job_suggested_param_set DROP INDEX job_execution_id_2;
UPDATE job_suggested_param_set jsps INNER JOIN job_execution je on jsps.fitness_job_execution_id = je.id set jsps.fitness_job_execution_id = NULL where je.job_exec_id is NULL;
ALTER TABLE job_suggested_param_value DROP INDEX job_execution_id;
ALTER TABLE job_suggested_param_set ADD CONSTRAINT job_suggested_param_set_f2 FOREIGN KEY (fitness_job_execution_id) REFERENCES job_execution(id);
ALTER TABLE job_suggested_param_set ADD CONSTRAINT job_suggested_param_set_f3 FOREIGN KEY (job_definition_id) REFERENCES job_definition (id);
DELETE from job_execution where job_exec_id is null;
ALTER TABLE job_suggested_param_set CHANGE param_set_state param_set_state enum('CREATED','SENT','EXECUTED','FITNESS_COMPUTED','DISCARDED') NOT NULL COMMENT 'state of this execution parameter set';

/**
 * Change NULL Allowable fields to have not null values
 */
ALTER TABLE job_execution CHANGE job_exec_id job_exec_id varchar(700) NOT NULL COMMENT 'unique job execution id from schedulers like azkaban, oozie etc';
ALTER TABLE job_execution CHANGE job_exec_url job_exec_url varchar(700) NOT NULL COMMENT 'job execution url from schedulers like azkaban, oozie etc';
ALTER TABLE job_execution DROP FOREIGN KEY job_execution_ibfk_2;
ALTER TABLE job_execution CHANGE flow_execution_id flow_execution_id int(10) unsigned NOT NULL COMMENT 'foreign key from flow_execution table';
ALTER TABLE job_execution ADD CONSTRAINT job_execution_ibfk_2 FOREIGN KEY (`flow_execution_id`) REFERENCES `flow_execution` (`id`);
ALTER TABLE job_execution CHANGE execution_state execution_state enum('SUCCEEDED','FAILED','NOT_STARTED','IN_PROGRESS','CANCELLED') NOT NULL COMMENT 'current state of execution of the job ';

/**
 * Table to have mapping between suggested param set and execution of the job
 */
CREATE TABLE IF NOT EXISTS tuning_job_execution_param_set (
  job_suggested_param_set_id int(10) unsigned NOT NULL COMMENT 'foreign key from job_suggested_param_set table',
  job_execution_id int(10) unsigned NOT NULL COMMENT 'foreign key from job_execution table',
  tuning_enabled tinyint(4) NOT NULL COMMENT 'Is tuning enabled for the execution',
  created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY tuning_job_execution_param_set_U1 (job_suggested_param_set_id, job_execution_id),
  CONSTRAINT tuning_job_execution_param_set_f1 FOREIGN KEY (job_suggested_param_set_id) REFERENCES job_suggested_param_set (id),
  CONSTRAINT tuning_job_execution_param_set_f2 FOREIGN KEY (job_execution_id) REFERENCES job_execution (id)
) ENGINE=InnoDB;

ALTER TABLE job_suggested_param_value ADD COLUMN job_suggested_param_set_id int(10) unsigned NOT NULL COMMENT 'foreign key from job_suggested_param_set table' after id;
UPDATE job_suggested_param_value SET job_suggested_param_set_id = job_execution_id;
ALTER TABLE job_suggested_param_value DROP COLUMN job_execution_id;
ALTER TABLE job_suggested_param_value ADD UNIQUE KEY job_suggested_param_value_u1 (job_suggested_param_set_id, tuning_parameter_id);
INSERT INTO tuning_job_execution_param_set (job_suggested_param_set_id, job_execution_id, tuning_enabled) SELECT id, fitness_job_execution_id, true from job_suggested_param_set where fitness_job_execution_id is not null;


/**
 * Table to have dynamic parameter constraints. This table will be used in IPSO
 */
CREATE TABLE IF NOT EXISTS tuning_parameter_constraint (
  id int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Auto increment unique id',
  job_definition_id int(10) unsigned NOT NULL COMMENT 'Job Definition ID',
  tuning_parameter_id int(100)  unsigned NOT NULL COMMENT 'tuning_parameter_id ',
  constraint_type enum('BOUNDARY','INTERDEPENDENT') NOT NULL COMMENT 'Constraint ID',
  lower_bound double COMMENT 'Lower bound of parameter',
  upper_bound double COMMENT 'Upper bound of parameter',
  created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  CONSTRAINT tuning_parameter_constraint_ibfk_1 FOREIGN KEY (job_definition_id) REFERENCES tuning_job_definition (job_definition_id),
  CONSTRAINT tuning_parameter_constraint_ibfk_2 FOREIGN KEY (tuning_parameter_id) REFERENCES tuning_parameter (id)
) ENGINE=InnoDB;

create index index_tuning_parameter_constraint on tuning_parameter_constraint (job_definition_id);

/**
 * Changes specific to Unified Architecture and HBT
 */
ALTER TABLE job_suggested_param_set ADD COLUMN is_param_set_suggested tinyint(4) default 0 NOT NULL COMMENT 'Is parameter set suggested' AFTER is_param_set_default;
ALTER TABLE job_suggested_param_set ADD COLUMN is_param_set_best tinyint(4) DEFAULT 0 NOT NULL COMMENT 'Is parameter set best' AFTER is_param_set_default;
ALTER TABLE job_suggested_param_set ADD COLUMN is_manually_overriden_parameter tinyint(4) default 0 NOT NULL COMMENT 'Is parameter set is manually overriden' AFTER is_param_set_best;
ALTER TABLE tuning_job_definition ADD COLUMN auto_apply tinyint(4) NOT NULL COMMENT 'auto apply is enabled or not ' AFTER tuning_enabled;
ALTER TABLE tuning_job_definition ADD COLUMN tuning_disabled_reason text NULL COMMENT 'reason for disabling tuning, if any' AFTER allowed_max_execution_time_percent;
ALTER TABLE tuning_job_definition ADD COLUMN number_of_iterations double DEFAULT 10 COMMENT 'number of iterations' AFTER tuning_disabled_reason;
ALTER TABLE tuning_algorithm MODIFY COLUMN optimization_algo enum('PSO','PSO_IPSO','HBT') NOT NULL COMMENT 'optimization algorithm name e.g. PSO' ;
INSERT INTO tuning_algorithm VALUES (3, 'PIG', 'PSO_IPSO', '3', 'RESOURCE', current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_algorithm VALUES (4, 'PIG', 'HBT', '4', 'RESOURCE', current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_algorithm VALUES (5, 'SPARK', 'HBT', '1', 'RESOURCE', current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (10,'mapreduce.task.io.sort.mb',3,100,50,1920,50, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (11,'mapreduce.map.memory.mb',3,2048,1024,8192,1024, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (12,'mapreduce.task.io.sort.factor',3,10,10,150,10 ,0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (13,'mapreduce.map.sort.spill.percent',3,0.8,0.6,0.9,0.1, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (14,'mapreduce.reduce.memory.mb',3,2048,1024,8192,1024, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (15,'mapreduce.reduce.java.opts',3,1536,500,6144,64, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (16,'mapreduce.map.java.opts',3,1536,500,6144,64, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (17,'mapreduce.input.fileinputformat.split.maxsize',3,536870912,536870912,536870912,128, 1, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (18,'pig.maxCombinedSplitSize',3,536870912,536870912,536870912,128, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (19,'mapreduce.map.memory.mb',4,2048,1024,8192,1024, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (20,'mapreduce.map.java.opts',4,1536,500,6144,64, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (21,'spark.executor.memory',5,2048,1024,8192,1024, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (22,'spark.driver.memory',5,2048,1024,8192,1024, 0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (23,'spark.executor.cores',5,1,1,5,1,0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (24,'spark.memory.fraction',5,0.05,0.05,0.9,0.01,0, current_timestamp(0), current_timestamp(0));

/**
 * Changes specific to modify constraints name .
 */
ALTER TABLE flow_definition DROP INDEX flow_def_id;
ALTER TABLE flow_definition ADD CONSTRAINT flow_definition_u1 UNIQUE (flow_def_id);
ALTER TABLE job_definition CHANGE COLUMN job_def_url job_def_url varchar(700) NOT NULL COMMENT 'job definition URL from scheduler like azkaban, oozie, appworx etc' AFTER job_def_id;
ALTER TABLE job_definition DROP INDEX job_def_id;
ALTER TABLE job_definition ADD CONSTRAINT job_definition_u1 UNIQUE (job_def_id);
ALTER TABLE job_suggested_param_set CHANGE COLUMN fitness_job_execution_id fitness_job_execution_id  int(10) unsigned DEFAULT NULL AFTER fitness;
ALTER TABLE job_suggested_param_set CHANGE id id int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Auto increment unique id';
ALTER TABLE job_suggested_param_set CHANGE job_definition_id job_definition_id int(10) unsigned NOT NULL COMMENT 'foreign key from job_definition table' ;
ALTER TABLE job_suggested_param_set CHANGE are_constraints_violated are_constraints_violated tinyint(4) NOT NULL DEFAULT '0' COMMENT 'are constraints violated for the parameter set' ;
ALTER TABLE job_suggested_param_set CHANGE is_param_set_default is_param_set_default tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Is parameter set default' ;
ALTER TABLE job_suggested_param_set CHANGE is_param_set_suggested is_param_set_suggested tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Is parameter set suggested' AFTER  is_param_set_default ;
ALTER TABLE job_suggested_param_set CHANGE fitness_job_execution_id fitness_job_execution_id int(10) unsigned DEFAULT NULL COMMENT 'foreign key from job_execution table' ;
ALTER TABLE job_suggested_param_set DROP FOREIGN KEY job_suggested_param_set_f2;
ALTER TABLE job_suggested_param_set DROP FOREIGN KEY `job_suggested_param_set_ibfk_1`, ADD CONSTRAINT `job_suggested_param_set_f1` FOREIGN KEY (`tuning_algorithm_id`) REFERENCES `tuning_algorithm` (`id`);
ALTER TABLE job_suggested_param_set DROP INDEX job_suggested_param_set_f3, ADD INDEX index_tje_job_definition_id (job_definition_id);
ALTER TABLE job_suggested_param_set DROP INDEX index_tje_job_execution_id;
ALTER TABLE  job_suggested_param_value ADD CONSTRAINT  job_suggested_param_value_f1  FOREIGN KEY (job_suggested_param_set_id) REFERENCES job_suggested_param_set(id);
-- ALTER TABLE tuning_algorithm ADD CONSTRAINT tuning_algorithm_u1 UNIQUE (optimization_algo,optimization_algo_version);
UPDATE tuning_job_definition SET auto_apply=1;

/*
  Adding a column in tuning job definition for show recommendation count
 */
alter table tuning_job_definition add column show_recommendation_count INT default 0;
