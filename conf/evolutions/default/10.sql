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

CREATE TABLE IF NOT EXISTS exception_fingerprinting (
    id int(11) unsigned NOT NULL AUTO_INCREMENT,
    app_id varchar(50) DEFAULT NULL COMMENT 'Application id assigned by RM to jobs',
    task_id varchar(50) DEFAULT NULL COMMENT 'Id of task belonging to a particular job',
    flow_exec_url varchar(800) NOT NULL COMMENT 'URL of the flow execution on Azkaban',
    job_name varchar(255) NOT NULL COMMENT 'Name of the job belonging to the flow',
    exception_type enum('FLOW', 'SCHEDULER', 'SCRIPT', 'MR', 'KILL', 'MRJOB', 'MRTASK', 'SPARK', 'DRIVER') NOT NULL COMMENT 'Reason of the exception',
    exception_log varchar(5120) NOT NULL COMMENT 'Stacktrace of the exception ',
    created_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation timestamp for helping in purge of stale exception fingerprinting data',
    PRIMARY KEY(id)
);
create index exception_fingerprinting_i1 on exception_fingerprinting (flow_exec_url, job_name);


# --- !Downs
drop index exception_fingerprinting_i1 on exception_fingerprinting;
DROP TABLE exception_fingerprinting;

