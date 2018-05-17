# --- Support for auto tuning spark
# --- !Ups

ALTER TABLE tuning_algorithm ADD UNIQUE KEY tuning_algorithm_uk1(optimization_algo, optimization_algo_version);
ALTER TABLE tuning_job_execution ADD COLUMN is_param_set_best tinyint(4) default 0 NOT NULL;
ALTER TABLE tuning_job_definition ADD COLUMN tuning_disabled_reason text;

# --- !Downs
ALTER TABLE tuning_job_definition DROP COLUMN tuning_disabled_reason;
ALTER TABLE tuning_job_execution DROP COLUMN is_param_set_best;
ALTER TABLE tuning_algorithm DROP INDEX tuning_algorithm_uk1;