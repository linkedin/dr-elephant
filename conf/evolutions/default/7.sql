/*
 * Copyright 2019 LinkedIn Corp.
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

INSERT INTO tuning_parameter VALUES (25,'spark.dynamicAllocation.minExecutors',5,1,1,900,1,0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (26,'spark.dynamicAllocation.maxExecutors',5,1,1,900,1,0, current_timestamp(0), current_timestamp(0));
INSERT INTO tuning_parameter VALUES (27,'spark.executor.instances',5,1,1,900,1,0, current_timestamp(0), current_timestamp(0));


# --- !Ups

/**
 * This query make neccesssary steps to run exception fingerprinting
 */

ALTER TABLE job_execution ADD COLUMN auto_tuning_fault TINYINT(4) DEFAULT 0 AFTER input_size_in_bytes;

# --- !Downs

ALTER TABLE job_execution DROP COLUMN auto_tuning_fault;
