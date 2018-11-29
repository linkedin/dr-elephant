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
 * This query make neccesssary steps to run Unified Architecture
 */

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

alter table tuning_job_definition add column show_recommendation_count INT default 0;

# --- !Downs
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=3;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=4;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=5;
DELETE FROM tuning_algorithm WHERE optimization_algo='PSO_IPSO' ;
ALTER TABLE tuning_algorithm MODIFY COLUMN optimization_algo enum('PSO') NOT NULL COMMENT 'optimization algorithm name e.g. PSO' ;
DROP TABLE tuning_parameter_constraint;
