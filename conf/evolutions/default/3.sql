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

# --- Indexing on queue for seach by queue feature
# --- !Ups

alter table yarn_app_result add column resource_used    BIGINT        UNSIGNED DEFAULT 0    COMMENT 'The resources used by the job in MB Seconds';
alter table yarn_app_result add column resource_wasted  BIGINT        UNSIGNED DEFAULT 0    COMMENT 'The resources wasted by the job in MB Seconds';
alter table yarn_app_result add column total_delay      BIGINT        UNSIGNED DEFAULT 0    COMMENT 'The total delay in starting of mappers and reducers';

# --- !Downs

alter table yarn_app_result drop resource_used;
alter table yarn_app_result drop resource_wasted;
alter table yarn_app_result drop total_delay;




