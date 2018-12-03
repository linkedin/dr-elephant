insert into yarn_app_result
    (id,name,username,queue_name,start_time,finish_time,tracking_url,job_type,severity,score,workflow_depth,scheduler,job_name,job_exec_id,flow_exec_id,job_def_id,flow_def_id,job_exec_url,flow_exec_url,job_def_url,flow_def_url,resource_used,resource_wasted,total_delay) values
    ('application_1458194917883_1453361','Email Overwriter','growth','misc_default',1460980616502,1460980723925,'http://elephant.linkedin.com:19888/jobhistory/job/job_1458194917883_1453361','HadoopJava',0,0,0,'azkaban','overwriter-reminder2','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293','https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder','https://elephant.linkedin.com:8443/executor?execid=1654676&job=overwriter-reminder2&attempt=0','https://elephant.linkedin.com:8443/executor?execid=1654676','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder&job=overwriter-reminder2','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder', 100, 30, 20),
    ('application_1458194917883_1453362','Email Overwriter','metrics','misc_default',1460980823925,1460980923925,'http://elephant.linkedin.com:19888/jobhistory/job/job_1458194917883_1453362','HadoopJava',0,0,0,'azkaban','overwriter-reminder2','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293','https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder','https://elephant.linkedin.com:8443/executor?execid=1654677&job=overwriter-reminder2&attempt=0','https://elephant.linkedin.com:8443/executor?execid=1654677','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder&job=overwriter-reminder2','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder', 200, 40, 10);

insert into yarn_app_heuristic_result(id,yarn_app_result_id,heuristic_class,heuristic_name,severity,score) values
(137594512,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic','Mapper Skew',0,0),
(137594513,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic','Mapper GC',0,0),
(137594516,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic','Mapper Time',0,0),
(137594520,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic','Mapper Speed',0,0),
(137594523,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic','Mapper Spill',0,0),
(137594525,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic','Mapper Memory',0,0),
(137594530,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic','Reducer Skew',0,0),
(137594531,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic','Reducer Time',0,0),
(137594534,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic','Reducer GC',0,0),
(137594537,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic','Reducer Memory',0,0),
(137594540,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic','Shuffle & Sort',0,0),
(137594612,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic','Mapper Skew',0,0),
(137594613,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic','Mapper GC',0,0),
(137594616,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic','Mapper Time',0,0),
(137594620,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic','Mapper Speed',0,0),
(137594623,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic','Mapper Spill',0,0),
(137594625,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic','Mapper Memory',0,0),
(137594630,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic','Reducer Skew',0,0),
(137594631,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic','Reducer Time',0,0),
(137594634,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic','Reducer GC',0,0),
(137594637,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic','Reducer Memory',0,0),
(137594640,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic','Shuffle & Sort',0,0);

insert into yarn_app_heuristic_result_details (yarn_app_heuristic_result_id,name,value,details) values
(137594512,'Group A','1 tasks @ 4 MB avg','NULL'),
(137594512,'Group B','1 tasks @ 79 MB avg','NULL'),
(137594512,'Number of tasks','2','NULL'),
(137594513,'Avg task CPU time (ms)','11510','NULL'),
 (137594513,'Avg task GC time (ms)','76','NULL'),
 (137594513,'Avg task runtime (ms)','11851','NULL'),
 (137594513,'Number of tasks','2','NULL'),
 (137594513,'Task GC/CPU ratio','0.006602953953084275 ','NULL'),
 (137594516,'Average task input size','42 MB','NULL'),
 (137594516,'Average task runtime','11 sec','NULL'),
 (137594516,'Max task runtime','12 sec','NULL'),
 (137594516,'Min task runtime','11 sec','NULL'),
 (137594516,'Number of tasks','2','NULL'),
 (137594520,'Median task input size','42 MB','NULL'),
 (137594520,'Median task runtime','11 sec','NULL'),
 (137594520,'Median task speed','3 MB/s','NULL'),
 (137594520,'Number of tasks','2','NULL'),
 (137594520,'Total input size in MB','58.65','NULL'),
 (137594523,'Avg output records per task','56687','NULL'),
 (137594523,'Avg spilled records per task','79913','NULL'),
 (137594523,'Number of tasks','2','NULL'),
 (137594523,'Ratio of spilled records to output records','1.4097111356119074','NULL'),
 (137594525,'Avg Physical Memory (MB)','522','NULL'),
 (137594525,'Avg task runtime','11 sec','NULL'),
 (137594525,'Avg Virtual Memory (MB)','3307','NULL'),
 (137594525,'Max Physical Memory (MB)','595','NULL'),
 (137594525,'Max Total Committed Heap Usage Memory (MB)','427','NULL'),
 (137594525,'Max Virtual Memory (MB)','1426','NULL'),
 (137594525,'Min Physical Memory (MB)','449','NULL'),
 (137594525,'Number of tasks','2','NULL'),
 (137594525,'Requested Container Memory','2 GB','NULL'),
 (137594530,'Group A','11 tasks @ 868 KB avg','NULL'),
 (137594530,'Group B','9 tasks @ 883 KB avg ','NULL'),
 (137594530,'Number of tasks','20','NULL'),
 (137594531,'Avg task CPU time (ms)','8912','NULL'),
 (137594531,'Avg task GC time (ms)','73','NULL'),
 (137594531,'Avg task runtime (ms)','11045','NULL'),
 (137594531,'Number of tasks','20','NULL'),
 (137594531,'Task GC/CPU ratio','0.008191202872531419 ','NULL'),
 (137594534,'Average task runtime','11 sec','NULL'),
 (137594534,'Max task runtime','14 sec','NULL'),
 (137594534,'Min task runtime','8 sec','NULL'),
 (137594534,'Number of tasks','20','NULL'),
 (137594537,'Avg Physical Memory (MB)','416','NULL'),
 (137594537,'Avg task runtime','11 sec','NULL'),
 (137594537,'Avg Virtual Memory (MB)','3326','NULL'),
 (137594537,'Max Physical Memory (MB)','497','NULL'),
 (137594537,'Max Total Committed Heap Usage Memory (MB)','300','NULL'),
 (137594537,'Max Virtual Memory (MB)','1350','NULL'),
 (137594537,'Min Physical Memory (MB)','354','NULL'),
 (137594537,'Number of tasks','20','NULL'),
 (137594537,'Requested Container Memory','2 GB','NULL'),
 (137594540,'Average code runtime','1 sec','NULL'),
 (137594540,'Average shuffle time','9 sec (5.49x)','NULL'),
 (137594540,'Average sort time','(0.04x)','NULL'),
 (137594540,'Number of tasks','20','NULL'),
 (137594612,'Group A','1 tasks @ 4 MB avg','NULL'),
 (137594612,'Group B','1 tasks @ 79 MB avg','NULL'),
 (137594612,'Number of tasks','2','NULL'),
 (137594613,'Avg task CPU time (ms)','11510','NULL'),
 (137594613,'Avg task GC time (ms)','76','NULL'),
 (137594613,'Avg task runtime (ms)','11851','NULL'),
 (137594613,'Number of tasks','2','NULL'),
 (137594613,'Task GC/CPU ratio','0.006602953953084275 ','NULL'),
 (137594616,'Average task input size','42 MB','NULL'),
 (137594616,'Average task runtime','11 sec','NULL'),
 (137594616,'Max task runtime','12 sec','NULL'),
 (137594616,'Min task runtime','11 sec','NULL'),
 (137594616,'Number of tasks','2','NULL'),
 (137594620,'Median task input size','42 MB','NULL'),
 (137594620,'Median task runtime','11 sec','NULL'),
 (137594620,'Median task speed','3 MB/s','NULL'),
 (137594620,'Number of tasks','2','NULL'),
 (137594620,'Total input size in MB','58.65','NULL'),
 (137594623,'Avg output records per task','56687','NULL'),
 (137594623,'Avg spilled records per task','79913','NULL'),
 (137594623,'Number of tasks','2','NULL'),
 (137594623,'Ratio of spilled records to output records','1.4097111356119074','NULL'),
 (137594625,'Avg Physical Memory (MB)','522','NULL'),
 (137594625,'Avg task runtime','11 sec','NULL'),
 (137594625,'Avg Virtual Memory (MB)','3307','NULL'),
 (137594625,'Max Physical Memory (MB)','595','NULL'),
 (137594625,'Max Total Committed Heap Usage Memory (MB)','300','NULL'),
 (137594625,'Max Virtual Memory (MB)','2200','NULL'),
 (137594625,'Min Physical Memory (MB)','449','NULL'),
 (137594625,'Number of tasks','2','NULL'),
 (137594625,'Requested Container Memory','2 GB','NULL'),
 (137594630,'Group A','11 tasks @ 868 KB avg','NULL'),
 (137594630,'Group B','9 tasks @ 883 KB avg ','NULL'),
 (137594630,'Number of tasks','20','NULL'),
 (137594631,'Avg task CPU time (ms)','8912','NULL'),
 (137594631,'Avg task GC time (ms)','73','NULL'),
 (137594631,'Avg task runtime (ms)','11045','NULL'),
 (137594631,'Number of tasks','20','NULL'),
 (137594631,'Task GC/CPU ratio','0.008191202872531419 ','NULL'),
 (137594634,'Average task runtime','11 sec','NULL'),
 (137594634,'Max task runtime','14 sec','NULL'),
 (137594634,'Min task runtime','8 sec','NULL'),
 (137594634,'Number of tasks','20','NULL'),
 (137594637,'Avg Physical Memory (MB)','416','NULL'),
 (137594637,'Avg task runtime','11 sec','NULL'),
 (137594637,'Avg Virtual Memory (MB)','3326','NULL'),
 (137594637,'Max Physical Memory (MB)','497','NULL'),
 (137594637,'Max Total Committed Heap Usage Memory (MB)','300','NULL'),
 (137594637,'Max Virtual Memory (MB)','2100','NULL'),
 (137594637,'Min Physical Memory (MB)','354','NULL'),
 (137594637,'Number of tasks','20','NULL'),
 (137594637,'Requested Container Memory','2 GB','NULL'),
 (137594640,'Average code runtime','1 sec','NULL'),
 (137594640,'Average shuffle time','9 sec (5.49x)','NULL'),
 (137594640,'Average sort time','(0.04x)','NULL'),
 (137594640,'Number of tasks','20','NULL');

INSERT INTO flow_definition(id, flow_def_id, flow_def_url) VALUES
(10003,'https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow','https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow');

INSERT INTO job_definition(id, job_def_id, flow_definition_id, job_name, job_def_url, scheduler, username, created_ts, updated_ts) VALUES
(100003,'https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry',10003,'countByCountryFlow_countByCountry','https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry','azkaban','pkumar2','2018-02-12 08:40:42','2018-02-12 08:40:43');


INSERT INTO tuning_job_definition(job_definition_id, client, tuning_algorithm_id, tuning_enabled, average_resource_usage, average_execution_time, average_input_size_in_bytes, allowed_max_resource_usage_percent, allowed_max_execution_time_percent, created_ts, updated_ts, tuning_disabled_reason, number_of_iterations, auto_apply)
VALUES
(100003,'azkaban',4,1,null,5.178423333333334,324168876088,150,150,'2018-02-12 08:40:42','2018-02-12 08:40:43', NULL, 5, true);




INSERT INTO flow_execution(id, flow_exec_id, flow_exec_url, flow_definition_id) VALUES
(1541,'https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293',10003);

INSERT INTO job_execution(id, job_exec_id, job_exec_url, job_definition_id, flow_execution_id, execution_state, resource_usage, execution_time, input_size_in_bytes, created_ts, updated_ts) VALUES
(1541,'https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0',100003,1541,'SUCCEEDED',21.132545572916666,3.2694833333333335,324713861757,'2018-02-14 05:30:42','2018-02-14 05:30:42');


INSERT INTO yarn_app_result VALUES
('application_1529108724527_640141','PigLatin:data_simple.pig','pkumar2','sna_default',1535553409318,1535553457517,'http://ltx1-farojh01.grid.linkedin.com:19888/jobhistory/job/job_1529108724527_640141','Pig',1,0,0,'azkaban','dimDataSimpleFlow_dimDataSimple','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1088630&job=dimDataSimpleFlow_dimDataSimple&attempt=0','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1088630','https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow&job=dimDataSimpleFlow_dimDataSimple','https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1088630&job=dimDataSimpleFlow_dimDataSimple&attempt=0','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1088630','https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow&job=dimDataSimpleFlow_dimDataSimple','https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow',104448,19227,9384);

INSERT INTO yarn_app_heuristic_result VALUES (16093,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic','Mapper Skew',0,0),(16094,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic','Mapper GC',0,0),(16095,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic','Mapper Time',0,0),(16096,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.ConfigurationHeuristic','MapReduceConfiguration',1,0),(16097,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic','Mapper Speed',0,0),(16098,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic','Mapper Spill',0,0),(16099,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic','Mapper Memory',0,0),(16100,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic','Reducer Skew',0,0),(16101,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic','Reducer GC',0,0),(16102,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic','Reducer Time',0,0),(16103,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic','Reducer Memory',0,0),(16104,'application_1529108724527_640141','com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic','Shuffle & Sort',0,0);

INSERT INTO yarn_app_heuristic_result_details VALUES
(16097,'Median task input size','300 MB',NULL),(16097,'Median task runtime','25 sec',NULL),(16097,'Median task speed','9 MB/s',NULL),(16097,'Number of tasks','2',NULL),(16097,'Total input size in MB','601.4286088943481',NULL);


INSERT INTO flow_definition VALUES (10046,'https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow','https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow','2018-09-12 08:57:43','2018-09-11 20:27:44') ;

INSERT INTO job_definition VALUES (100046,'https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow&job=dimDataSimpleFlow_dimDataSimple','https://ltx1-faroaz01.grid.linkedin.com:8443/manager?project=tuning_hbt&flow=dimDataSimpleFlow&job=dimDataSimpleFlow_dimDataSimple',10046,'dimDataSimpleFlow_dimDataSimple','azkaban','pkumar2','2018-09-12 08:57:43','2018-09-11 20:27:44');

INSERT INTO tuning_job_definition VALUES (100046,'azkaban',3,1,1,NULL,1.15030667,1912288051,150,150,NULL,10,'2018-09-12 08:57:43','2018-09-11 20:27:49',0);

INSERT INTO flow_execution VALUES (1084,'https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122321','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122321',10046,'2018-09-12 08:57:43','2018-09-11 20:27:44'),(1085,'https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122375','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122375',10046,'2018-09-12 09:20:16','2018-09-11 20:50:16');

INSERT INTO job_execution VALUES (1084,'https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122321&job=dimDataSimpleFlow_dimDataSimple&attempt=0','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122321&job=dimDataSimpleFlow_dimDataSimple&attempt=0',100046,1084,'SUCCEEDED',0.6155555555555555,1.2017666666666666,1918012391,'2018-09-12 08:57:43','2018-09-11 20:35:58'),(1085,'https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122375&job=dimDataSimpleFlow_dimDataSimple&attempt=0','https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=1122375&job=dimDataSimpleFlow_dimDataSimple&attempt=0',100046,1085,'SUCCEEDED',0.15722222222222224,1.2016666666666667,1918012391,'2018-09-12 09:20:16','2018-09-11 20:58:03');