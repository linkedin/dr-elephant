# --- Support for auto tuning spark
# --- !Ups

ALTER TABLE tuning_algorithm ADD UNIQUE KEY (optimization_algo, optimization_algo_version);
ALTER TABLE tuning_job_execution ADD COLUMN is_param_set_best tinyint(4) default 0;
ALTER TABLE tuning_job_definition ADD COLUMN tuning_disabled_reason text;

# --- !Downs
