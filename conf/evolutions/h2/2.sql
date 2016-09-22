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

alter table yarn_app_result add column status varchar(10) not null COMMENT 'The status of the job';

update yarn_app_result set severity=severity+1;

update yarn_app_heuristic_result set severity=severity+1;

alter table yarn_app_heuristic_result drop constraint yarn_app_heuristic_result_f1;

alter table yarn_app_heuristic_result add constraint yarn_app_heuristic_result_f1 foreign key (yarn_app_result_id) references yarn_app_result (id) ON UPDATE CASCADE ON DELETE CASCADE;

alter table yarn_app_heuristic_result_details drop constraint yarn_app_heuristic_result_details_f1;

alter table yarn_app_heuristic_result_details add constraint yarn_app_heuristic_result_details_f1 foreign key (yarn_app_heuristic_result_id) references yarn_app_heuristic_result (id) ON UPDATE CASCADE ON DELETE CASCADE;

# --- !Downs

alter table yarn_app_result drop column status;

update yarn_app_result set severity=severity-1;

update yarn_app_heuristic_result set severity=severity-1;

alter table yarn_app_heuristic_result drop constraint yarn_app_heuristic_result_f1;

alter table yarn_app_heuristic_result add constraint yarn_app_heuristic_result_f1 foreign key (yarn_app_result_id) references yarn_app_result (id);

alter table yarn_app_heuristic_result_details drop constraint yarn_app_heuristic_result_details_f1;

alter table yarn_app_heuristic_result_details add constraint yarn_app_heuristic_result_details_f1 foreign key (yarn_app_heuristic_result_id) references yarn_app_heuristic_result (id);
