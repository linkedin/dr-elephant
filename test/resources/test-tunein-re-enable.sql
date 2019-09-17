INSERT INTO `flow_definition` (`id`, `flow_def_id`, `flow_def_url`, `created_ts`, `updated_ts`) VALUES
(1,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1','2019-05-28 22:07:43','2019-05-28 22:07:43'),
(2,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_2',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_2','2019-05-28 23:07:54','2019-05-28 23:07:54');

INSERT INTO `job_definition` (`id`, `job_def_id`, `job_def_url`, `flow_definition_id`, `job_name`, `scheduler`, `username`, `created_ts`, `updated_ts`)
VALUES (100149,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1&job=job_1',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_1&job=job_1',
1,'job_1','azkaban','metrics','2019-05-28 22:07:43','2019-05-28 22:07:43'),

(100150,'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_2&job=job_2',
'https://elephant.linkedin.com:8443/manager?project=testProject&flow=flow_2&job=job_2',
2,'job_2','azkaban','metrics','2019-05-28 23:07:54','2019-05-28 23:07:54');

INSERT INTO `tuning_algorithm` (`job_type`, `optimization_algo`, `optimization_algo_version`, `optimization_metric`, `created_ts`,
 `updated_ts`) VALUES
 ('PIG','PSO',1,'RESOURCE','2018-08-23 08:53:51','2018-08-23 08:53:51'),
 ('PIG','PSO_IPSO',3,'RESOURCE','2018-08-23 08:53:53','2018-08-23 08:53:53'),
 ('PIG','HBT',4,'RESOURCE','2018-08-29 13:16:25','2018-08-29 13:16:25'),
 ('SPARK','HBT',1,'RESOURCE','2018-09-04 07:26:29','2018-09-04 07:26:29');

INSERT INTO `tuning_job_definition` (`job_definition_id`, `client`, `tuning_algorithm_id`, `tuning_enabled`, `auto_apply`,
 `average_resource_usage`, `average_execution_time`, `average_input_size_in_bytes`, `allowed_max_resource_usage_percent`,
 `allowed_max_execution_time_percent`, `tuning_disabled_reason`, `number_of_iterations`, `created_ts`, `updated_ts`,
 `show_recommendation_count`) VALUES
 (100149,'azkaban',4,0,0,14.48426667,42.86533333,52739178496,150,150,'All Heuristics Passed',10,'2019-05-29 04:03:38',
 '2019-09-12 18:38:46',9),
 (100150,'azkaban',4,0,1,3920.58746667,43.52463333,8200657043456,150,150,'All Heuristics Passed',10,
'2019-05-29 03:06:47','2019-09-13 19:50:03',31);

INSERT INTO `flow_execution` (`id`, `flow_exec_id`, `flow_exec_url`, `flow_definition_id`, `created_ts`, `updated_ts`) VALUES
(1573,'https://elephant.linkedin.com:8443/executor?execid=5252291','https://elephant.linkedin.com:8443/executor?execid=5252291'
,2,'2019-05-29 03:06:48','2019-05-29 03:06:48'),
(1574,'https://elephant.linkedin.com:8443/executor?execid=5252449','https://elephant.linkedin.com:8443/executor?execid=5252449',
1,'2019-05-29 04:03:38','2019-05-29 04:03:39'),
(1575,'https://elephant.linkedin.com:8443/executor?execid=5252941',
'https://elephant.linkedin.com:8443/executor?execid=5252941',1,'2019-05-29 07:00:43','2019-05-29 07:00:44'),
(1576,'https://elephant.linkedin.com:8443/executor?execid=5253039','https://elephant.linkedin.com:8443/executor?execid=5253039',
2,'2019-05-29 07:32:46','2019-05-29 07:32:47');

INSERT INTO `job_execution`
(`id`, `job_exec_id`, `job_exec_url`, `job_definition_id`, `flow_execution_id`, `execution_state`, `resource_usage`,
 `execution_time`, `input_size_in_bytes`, `auto_tuning_fault`, `created_ts`, `updated_ts`) VALUES
 (1573,'https://elephant.linkedin.com:8443/executor?execid=5252291&job=job_2&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5252291&job=job_2&attempt=0',100150,1573,'SUCCEEDED',6712.747777777778,35.98485,0,0,'2019-05-29 03:06:48','2019-05-29 05:13:22'),
 (1574,'https://elephant.linkedin.com:8443/executor?execid=5252449&job=job_1&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5252449&job=job_1&attempt=0',100149,1574,'SUCCEEDED',16.90162326388889,44.608583333333335,0,0,'2019-05-29 04:03:38','2019-05-29 06:16:26'),
 (1575,'https://elephant.linkedin.com:8443/executor?execid=5252941&job=job_1&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5252941&job=job_1&attempt=0',100149,1575,
 'SUCCEEDED',16.791666666666668,49.040533333333336,0,0,'2019-05-29 07:00:43','2019-05-29 09:11:44'),
 (1576,'https://elephant.linkedin.com:8443/executor?execid=5253039&job=job_2&attempt=0',
 'https://elephant.linkedin.com:8443/executor?execid=5253039&job=job_2&attempt=0',100150,
 1576,'SUCCEEDED',5378.952222222222,74.86451666666666,0,0,'2019-05-29 07:32:46','2019-05-29 10:07:47');

INSERT INTO `job_suggested_param_set` (`id`, `job_definition_id`, `tuning_algorithm_id`, `param_set_state`, `are_constraints_violated`,
 `is_param_set_default`, `is_param_set_best`, `is_manually_overriden_parameter`, `is_param_set_suggested`, `fitness`,
  `fitness_job_execution_id`, `created_ts`, `updated_ts`) VALUES
  (1579,100149,4,'FITNESS_COMPUTED',0,1,0,0,0,0,1574,'2019-05-29 04:03:38','2019-09-15 11:20:17'),
  (1581,100149,4,'FITNESS_COMPUTED',0,0,1,0,1,112,1575,'2019-05-29 06:16:25','2019-05-29 09:11:44'),
  (1578,100150,4,'FITNESS_COMPUTED',0,1,0,0,0,149702,1573,'2019-05-29 03:06:48','2019-05-29 11:48:04'),
  (1580,100150,4,'FITNESS_COMPUTED',0,0,1,0,1,52588,1576,'2019-05-29 05:13:21','2019-05-29 17:08:44');
