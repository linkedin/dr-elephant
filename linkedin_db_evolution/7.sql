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
ALTER TABLE flow_definition ADD COLUMN created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE flow_definition ADD COLUMN updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE flow_execution ADD COLUMN created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE flow_execution ADD COLUMN updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE tuning_job_execution ADD COLUMN created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE tuning_job_execution ADD COLUMN updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE tuning_job_execution DROP FOREIGN KEY tuning_job_execution_ibfk_2;
ALTER TABLE tuning_job_execution RENAME to job_suggested_param_set;
ALTER TABLE job_suggested_param_set ADD COLUMN are_constraints_violated tinyint(4) default 0 NOT NULL after param_set_state;
ALTER TABLE job_suggested_param_set CHANGE is_default_execution is_param_set_default tinyint(4) default 0 NOT NULL;
ALTER TABLE job_suggested_param_set ADD COLUMN id int(10) DEFAULT 0 NOT NULL after job_execution_id;
UPDATE job_suggested_param_set set id = job_execution_id;
ALTER TABLE job_suggested_param_set ADD PRIMARY KEY(id);
ALTER TABLE job_suggested_param_set ADD COLUMN job_definition_id int(10) unsigned after id NOT NULL;
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
ALTER TABLE job_execution CHANGE job_exec_id job_exec_id varchar(700) NOT NULL COMMENT 'unique job execution id from schedulers like azkaban, oozie etc';
ALTER TABLE job_execution CHANGE job_exec_url job_exec_url varchar(700) NOT NULL COMMENT 'job execution url from schedulers like azkaban, oozie etc';
ALTER TABLE job_execution CHANGE flow_execution_id flow_execution_id int(10) unsigned NOT NULL COMMENT 'foreign key from flow_execution table';
ALTER TABLE job_execution CHANGE execution_state execution_state enum('SUCCEEDED','FAILED','NOT_STARTED','IN_PROGRESS','CANCELLED') NOT NULL COMMENT 'current state of execution of the job ';


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