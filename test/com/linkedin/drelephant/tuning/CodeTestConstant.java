package com.linkedin.drelephant.tuning;

/**
 * This class is utility class consist of contants which are required for
 * code analayis and shared across different test classes.
 */
public class CodeTestConstant {
  public static final String SOURCE_CODE= "SET mapreduce.input.fileinputformat.split.maxsize=536870912; \n"
      + "SET mapreduce.input.fileinputformat.split.minsize=268435456; \n" + "SET mapreduce.map.memory.mb = 2048; \n"
      + "SET mapreduce.map.java.opts = -Xmx1770m -XX:ParallelGCThreads=4; \n"
      + "SET mapreduce.reduce.memory.mb = 2048; \n"
      + "SET mapreduce.reduce.java.opts = -Xmx1770m -XX:ParallelGCThreads=4; \n" + " \n" + "USE ${db_name}; \n" + "\n"
      + " \n" + "CREATE TEMPORARY TABLE table1 AS \n" + "SELECT \n" + "  rome.id AS id, \n"
      + "  rome.member_id AS member_id\n" + "FROM \n" + "  $table7 rome \n"
      + "LEFT JOIN (SELECT * FROM $table2 WHERE member_id IS NOT NULL) member_data \n"
      + "  ON rome.member_id = member_data.member_id \n"
      + " AND COALESCE(rome.merge_source,'#N/A') = COALESCE(member_data.merge_source,'#N/A') \n" + "; \n" + " \n"
      + "-- Pick records \n" + "CREATE TEMPORARY TABLE table3 AS \n" + "SELECT \n" + "  id, \n" + "  member_id\n"
      + "FROM \n" + "  $table2 rome1 \n" + "WHERE id IS NOT NULL \n" + "AND member_id IS NULL \n" + "; \n" + " \n"
      + "-- Union the above two temporary tables. Since the tables are mutually exclusive, we do not have to dedup them \n"
      + "CREATE TEMPORARY TABLE table4 AS \n" + "SELECT * FROM table1 \n" + "UNION ALL \n" + "SELECT * FROM table3 \n"
      + "; \n" + " \n" + "-- Augment with Email optout information \n" + "CREATE TEMPORARY TABLE table5 AS \n"
      + "SELECT \n" + "  COALESCE(union_table.id, optout.id) AS id, \n"
      + "  COALESCE(union_table.member_id, optout.member_id) AS member_id\n" + "FROM \n" + "  table4 union_table \n"
      + "FULL OUTER JOIN $table6 optout \n" + "   ON union_table.join_key = optout.join_key \n" + "; \n" + " \n"
      + "DROP TABLE IF EXISTS $output_table; \n" + " \n" + "CREATE EXTERNAL TABLE IF NOT EXISTS $output_table ( \n"
      + "  id                                    string, \n" + "  member_id                             string\n"
      + ") \n" + "PARTITIONED BY (datepartition string) \n" + "STORED AS ORC \n" + "LOCATION '$output_hdfspath' \n"
      + "TBLPROPERTIES ( \n" + "  \"orc.compress\"=\"SNAPPY\" \n" + "); \n" + " \n" + "-- Augment \n"
      + "INSERT OVERWRITE TABLE ${output_table} PARTITION (datepartition='${date_partition}') \n" + "SELECT \n"
      + "  final.id, \n" + "  final.member_id\n" + "FROM \n" + "  (SELECT \n" + "    a.*, \n"
      + "    row_number() over(partition by case when a.id is null then a.member_id else a.id end \n"
      + "    order by a.id desc, a.member_id desc) as rank \n" + "  FROM \n" + "    (SELECT \n"
      + "      COALESCE(aug.id, nr.id) AS id, \n" + "      aug.member_id AS member_id, \n"
      + "      COALESCE(nr.accountid, aug.accountid) AS accountid\n" + "    FROM \n" + "      table5 aug \n"
      + "    FULL OUTER JOIN $stg_nova_roma nr \n" + "    ON aug.id = nr.id \n" + "    ) a \n" + "  ) final \n"
      + "WHERE \n" + "  final.rank = 1 \n" + "; ";
}
