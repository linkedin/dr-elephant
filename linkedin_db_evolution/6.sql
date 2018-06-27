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

ALTER TABLE tuning_algorithm ADD UNIQUE KEY tuning_algorithm_u1(optimization_algo, optimization_algo_version);
ALTER TABLE tuning_job_execution ADD COLUMN is_param_set_best tinyint(4) default 0 NOT NULL;
ALTER TABLE tuning_job_definition ADD COLUMN tuning_disabled_reason text;

# --- !Downs
ALTER TABLE tuning_job_definition DROP COLUMN tuning_disabled_reason;
ALTER TABLE tuning_job_execution DROP COLUMN is_param_set_best;
ALTER TABLE tuning_algorithm DROP INDEX tuning_algorithm_uk1;

