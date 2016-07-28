# --- Cascade deletes to make dropping data easier (1 step instead of 3)
# --- !Ups

ALTER TABLE yarn_app_heuristic_result DROP FOREIGN KEY yarn_app_heuristic_result_f1;
ALTER TABLE yarn_app_heuristic_result ADD CONSTRAINT yarn_app_heuristic_result_f1 FOREIGN KEY (yarn_app_result_id) REFERENCES yarn_app_result (id) ON DELETE CASCADE;
ALTER TABLE yarn_app_heuristic_result_details DROP FOREIGN KEY  yarn_app_heuristic_result_details_f1;
ALTER TABLE yarn_app_heuristic_result_details ADD CONSTRAINT yarn_app_heuristic_result_details_f1 FOREIGN KEY (yarn_app_heuristic_result_id) REFERENCES yarn_app_heuristic_result (id) ON DELETE CASCADE;

# --- !Downs

ALTER TABLE yarn_app_heuristic_result DROP FOREIGN KEY  yarn_app_heuristic_result_f1;
ALTER TABLE yarn_app_heuristic_result ADD CONSTRAINT yarn_app_heuristic_result_f1 FOREIGN KEY (yarn_app_result_id) REFERENCES yarn_app_result (id);
ALTER TABLE yarn_app_heuristic_result_details DROP FOREIGN KEY  yarn_app_heuristic_result_details_f1;
ALTER TABLE yarn_app_heuristic_result_details ADD CONSTRAINT yarn_app_heuristic_result_details_f1 FOREIGN KEY (yarn_app_heuristic_result_id) REFERENCES yarn_app_heuristic_result (id);
