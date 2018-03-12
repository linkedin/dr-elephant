# --- Indexing on severity,finish_time for count on welcome page
# --- !Ups

create index yarn_app_result_i9 on yarn_app_result (severity,finish_time);

# --- !Downs

drop index yarn_app_result_i9 on yarn_app_result;




