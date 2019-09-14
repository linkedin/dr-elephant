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
 * This makes table which is required to persist code level recommendation data 
 */



CREATE TABLE tuning_job_execution_code_recommendation (
  id int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Auto increment unique id',
  job_def_id varchar(700) DEFAULT NULL,
  job_exec_url varchar(700) DEFAULT NULL,
  code_location varchar(700) DEFAULT NULL,
  recommendation varchar(700) DEFAULT NULL,
  severity varchar(700) DEFAULT NULL,
  created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB

# --- !Downs
DROP TABLE tuning_job_execution_code_recommendation;
