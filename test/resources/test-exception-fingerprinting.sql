INSERT INTO exception_fingerprinting (app_id, task_id, flow_exec_url, job_name, exception_type, exception_log)
VALUES ('NA','NA','https://elephant.linkedin.com:8443/executor?execid=1','job_name_1','MR',
'stack_trace_1');

INSERT INTO exception_fingerprinting (app_id, task_id, flow_exec_url, job_name, exception_type, exception_log, is_helpful) VALUES
('job_id_1','task_id_1','https://elephant.linkedin.com:8443/executor?execid=2','job_name_2_1','MRTASK','stack_trace_2', null),
('job_id_1','NA','https://elephant.linkedin.com:8443/executor?execid=2','job_name_2_1','MRJOB','', null),
('NA','NA','https://elephant.linkedin.com:8443/executor?execid=2','job_name_2_1','MR','', null),
('application_id_1','NA','https://elephant.linkedin.com:8443/executor?execid=2','job_name_2_2','DRIVER','[{"exceptionID":-447904366,"exceptionName":"Caused by: java.lang.ClassNotFoundException","weightOfException":5,"exceptionStackTrace":"ABCD","exceptionSource":"DRIVER"}]', null),
('NA','NA','https://elephant.linkedin.com:8443/executor?execid=2','job_name_2_2','SPARK','', null);