# --- Indexing on queue for seach by queue feature
# --- !Ups

alter table yarn_app_result add column resource_used    BIGINT        UNSIGNED DEFAULT 0    COMMENT 'The resources used by the job in MB Seconds';
alter table yarn_app_result add column resource_wasted  BIGINT        UNSIGNED DEFAULT 0    COMMENT 'The resources wasted by the job in MB Seconds';
alter table yarn_app_result add column total_delay      BIGINT        UNSIGNED DEFAULT 0    COMMENT 'The total delay in starting of mappers and reducers';

# --- !Downs

alter table yarn_app_result drop resource_used;
alter table yarn_app_result drop resource_wasted;
alter table yarn_app_result drop total_delay;




