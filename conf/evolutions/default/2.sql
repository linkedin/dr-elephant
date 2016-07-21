# --- Indexing on queue for seach by queue feature
# --- !Ups


ALTER TABLE yarn_app_result ADD COLUMN parents VARCHAR(255)  DEFAULT NULL          COMMENT 'Comma separated list of parents of the application';

create table yarn_app_job_name_map(
  flow_exec_id    VARCHAR(255)  NOT NULL DEFAULT ''   COMMENT 'A unique reference to a specific flow execution. This should filter all applications fired by a particular flow execution. Note that if the scheduler supports sub-workflows, then this ID should be the super parent flow execution id that triggered the the applications and sub-workflows.',
  job_name        VARCHAR(255)  NOT NULL DEFAULT ''   COMMENT 'The name of the job in the flow to which this app belongs',
  job_name_id     INT(4) NOT NULL                     COMMENT 'The unque id for every job of a flow execution',
  job_innodes     VARCHAR(100)    DEFAULT NULL         COMMENT 'A comma separated list of innodes of a job',
  PRIMARY KEY (flow_exec_id, job_name)
);

# --- !Downs

ALTER TABLE yarn_app_result DROP COLUMN parents;

DROP TABLE yarn_app_job_name_map;
