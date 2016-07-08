package com.linkedin.drelephant.exceptions;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class AzkabanJobLogAnalyzerTest {

  private String succeededAzkabanJobLog = "24-06-2016 03:12:53 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Starting job extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer at 1466763173873\n"
      + "24-06-2016 03:12:53 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - job JVM args: -Dazkaban.flowid=extractCrawlCompanyIndustryClassificationFlow -Dazkaban.execid=557260 -Dazkaban.jobid=extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer\n"
      + "24-06-2016 03:12:53 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Building hadoopJava job executor. \n"
      + "24-06-2016 03:12:53 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Initiating hadoop security manager.\n"
      + "24-06-2016 03:12:55 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Token id: \u0007bchalla\u0004yarn?azkaban/ltx1-holdemazexec02.grid.linkedin.com@GRID.LINKEDIN.COM���\u0001U���������S���\u0001U������dS���L���������\u0001X\n"
      + "24-06-2016 03:12:55 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Token service: ltx1-holdemnn01.grid.linkedin.com:9000\n"
      + "24-06-2016 03:12:55 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Cancelling namenode token \u0007bchalla\u0004yarn?azkaban/ltx1-holdemazexec02.grid.linkedin.com@GRID.LINKEDIN.COM���\u0001U���������S���\u0001U������dS���L���������\u0001X\n"
      + "24-06-2016 03:12:55 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Finishing job extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer attempt: 0 at 1466763175040 with status SUCCEEDED";

  private String killedAzkabanJobLog = "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Starting job feature-exploration_create-index-map at 1467158300703\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - job JVM args: -Dazkaban.flowid=feature-exploration -Dazkaban.execid=594574 -Dazkaban.jobid=feature-exploration_create-index-map\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Building spark job executor. \n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Initiating hadoop security manager.\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Loading hadoop security manager azkaban.security.HadoopSecurityManager_H_2_0\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Need to proxy. Getting tokens.\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Getting hadoop tokens based on props for vgulati\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Pre-fetching JH token from job history server\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Created JH token: Kind: MR_DELEGATION_TOKEN, Service: ltx1-holdemjh01.grid.linkedin.com:10020, Ident: (owner=azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM, renewer=yarn, realUser=, issueDate=1467158300747, maxDate=1467763100747, sequenceNumber=677674, masterKeyId=25)\n"
      + "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Token kind: MR_DELEGATION_TOKEN\n"
      + "28-06-2016 17:58:05 PDT feature-exploration_create-index-map ERROR - Kill has been called.\n"
      + "28-06-2016 17:58:05 PDT feature-exploration_create-index-map INFO - 16/06/29 00:58:05 INFO util.Utils: Shutdown hook called\n"
      + "28-06-2016 17:58:05 PDT feature-exploration_create-index-map INFO - 16/06/29 00:58:05 INFO util.Utils: Deleting directory /grid/a/mapred/tmp/spark-39ad1db3-27f2-4d8d-b834-8d63c955cf39\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Cancel called.  Killing the Spark job on the cluster\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - log file path is: /export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/594574/_job.594574.feature-exploration_create-index-map.log\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Process completed unsuccessfully in 3585 seconds.\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map ERROR - caught error running the job\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Found token file.  Setting mapreduce.job.credentials.binary to /grid/a/mapred/tmp/mr-azkaban8155942346693885627.token\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Current logged in user is azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - security enabled, proxying as user vgulati\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Got token: Kind: RM_DELEGATION_TOKEN, Service: ltx1-holdemrm01.grid.linkedin.com:8032, Ident: (owner=vgulati, renewer=yarn, realUser=azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM, issueDate=1467158300848, maxDate=1467763100848, sequenceNumber=1699445, masterKeyId=66)\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token kind: RM_DELEGATION_TOKEN\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token id: \n"
      + "\u0007vgulati\u0012\u0004yarn\u001A?azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM ���������������*(���������������*0������g8B\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token service: ltx1-holdemrm01.grid.linkedin.com:8032\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Cancelling mr job tracker token \n"
      + "\u0007vgulati\u0012\u0004yarn\u001A?azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM ���������������*(���������������*0������g8B\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Got token: Kind: MR_DELEGATION_TOKEN, Service: ltx1-holdemjh01.grid.linkedin.com:10020, Ident: (owner=azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM, renewer=yarn, realUser=, issueDate=1467158300747, maxDate=1467763100747, sequenceNumber=677674, masterKeyId=25)\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token kind: MR_DELEGATION_TOKEN\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token id: ?azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM\u0004yarn���\u0001U���s\bK���\u0001U���\u007F���K���\n"
      + "W*\u0019\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token service: ltx1-holdemjh01.grid.linkedin.com:10020\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Cancelling jobhistoryserver mr token ?azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM\u0004yarn���\u0001U���s\bK���\u0001U���\u007F���K���\n"
      + "W*\u0019\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Got token: Kind: HDFS_DELEGATION_TOKEN, Service: ltx1-holdemnn01.grid.linkedin.com:9000, Ident: (HDFS_DELEGATION_TOKEN token 5466954 for vgulati)\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token kind: HDFS_DELEGATION_TOKEN\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token id: \u0007vgulati\u0004yarn?azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM���\u0001U���s\b_���\u0001U���\u007F���_���SkJ���\u0001\\\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token service: ltx1-holdemnn01.grid.linkedin.com:9000\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Cancelling namenode token \u0007vgulati\u0004yarn?azkaban/ltx1-holdemazexec01.grid.linkedin.com@GRID.LINKEDIN.COM���\u0001U���s\b_���\u0001U���\u007F���_���SkJ���\u0001\\\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map ERROR - Job run failed!\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map ERROR - java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException cause: java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Finishing job feature-exploration_create-index-map attempt: 0 at 1467161886022 with status KILLED\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - applicationIds to kill: [application_1466048666726_642278]\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - start klling application: application_1466048666726_642278\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - successfully killed application: application_1466048666726_642278";

  private String mrLevelFailedAzkabanJobLog = "24-06-2016 03:12:19 PDT help_center_sessions INFO - Starting job help_center_sessions at 1466763139993\n"
      + "24-06-2016 03:12:19 PDT help_center_sessions INFO - job JVM args: -Dazkaban.flowid=help_center -Dazkaban.execid=557257 -Dazkaban.jobid=help_center_sessions -Dmapred.reduce.tasks=${mapred.reduce.tasks} -Dmapred.job.queue.name=${mapred.job.queue.name} -Dmapred.child.java.opts=\"${mapred.child.java.opts}\" -Dmapred.min.split.size=${mapred.min.split.size} -Dmapred.max.split.size=${mapred.max.split.size} -Dmapred.compress.map.output=${mapred.compress.map.output} -Dmapred.output.compress=${mapred.output.compress} -Dmapred.map.output.compression.codec=${mapred.map.output.compression.codec} -Ddfs.block.size=${dfs.block.size} -Djava.io.tmpdir=/grid/a/mapred/tmp -Dpig.usenewlogicalplan=false -Dpig.splitCombination=true -Dpig.additional.jars=${pig.additional.jars} -Dpig.maxCombinedSplitSize=${mapred.min.split.size} -Djava.library.path=${HADOOP_HOME}/lib/native/Linux-amd64-64 -Dmapred.create.symlink=yes -Dmapreduce.reduce.input.limit=${mapreduce.reduce.input.limit} -Dmapreduce.reduce.shuffle.input.buffer.percent=0.5 -Ddfs.umaskmode=${dfs.umaskmode} -Dudf.import.list=${udf.import.list} -Dmapreduce.job.complete.cancel.delegation.tokens=${mapreduce.job.complete.cancel.delegation.tokens} -Dmapreduce.job.user.classpath.first=${mapreduce.job.user.classpath.first} -Dmapreduce.map.memory.mb=${mapreduce.map.memory.mb} -Dmapreduce.reduce.memory.mb=${mapreduce.reduce.memory.mb} -Dyarn.app.mapreduce.am.staging-dir=${yarn.tmp.dir} -Dhive.querylog.location=. -Dhive.mapred.supports.subdirectories=true -Dhive.aux.jars.path=${hive.aux.jars.path} -Dhive.exec.scratchdir=/tmp/hive-${user.to.proxy}\n"
      + "24-06-2016 03:12:19 PDT help_center_sessions INFO - Building hadoopJava job executor. \n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Initiating hadoop security manager.\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Loading hadoop security manager azkaban.security.HadoopSecurityManager_H_2_0\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Need to proxy. Getting tokens.\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Getting hadoop tokens based on props for ragepati\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Pre-fetching default Hive MetaStore token from hive\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - hive.metastore.uris: thrift://ltx1-holdemhcat01.grid.linkedin.com:7552\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - hive.metastore.sasl.enabled: true\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - hive.metastore.kerberos.principal: hcat/_HOST@GRID.LINKEDIN.COM\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Created hive metastore token: ZwAIcmFnZXBhdGkHYXprYWJhbj9hemthYmFuL2x0eDEtaG9sZGVtYXpleGVjMDIuZ3JpZC5saW5rZWRpbi5jb21AR1JJRC5MSU5LRURJTi5DT02KAVWB5VxvigFVpfHgb40EHn-OARUUxSco-f83MaSx1l-Qh74mI27EAycVSElWRV9ERUxFR0FUSU9OX1RPS0VOAA\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token kind: HIVE_DELEGATION_TOKEN\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token id: [B@5fc6f09a\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token service: \n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Pre-fetching JH token from job history server\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Created JH token: Kind: MR_DELEGATION_TOKEN, Service: ltx1-holdemjh01.grid.linkedin.com:10020, Ident: (owner=azkaban/ltx1-holdemazexec02.grid.linkedin.com@GRID.LINKEDIN.COM, renewer=yarn, realUser=, issueDate=1466763140314, maxDate=1467367940314, sequenceNumber=505625, masterKeyId=20)\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token kind: MR_DELEGATION_TOKEN\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token id: [B@71ae109a\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token service: ltx1-holdemjh01.grid.linkedin.com:10020\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Here is the props for obtain.namenode.token: true\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Getting DFS token from hdfs://ltx1-holdemnn01.grid.linkedin.com:9000\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Created DFS token: Kind: HDFS_DELEGATION_TOKEN, Service: ltx1-holdemnn01.grid.linkedin.com:9000, Ident: (HDFS_DELEGATION_TOKEN token 5017233 for ragepati)\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token kind: HDFS_DELEGATION_TOKEN\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token id: [B@7e2249b3\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token service: ltx1-holdemnn01.grid.linkedin.com:9000\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - other_namenodes was not configured\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Pre-fetching JT token from JobTracker\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Created JT token: Kind: RM_DELEGATION_TOKEN, Service: ltx1-holdemrm01.grid.linkedin.com:8032, Ident: (owner=ragepati, renewer=yarn, realUser=azkaban/ltx1-holdemazexec02.grid.linkedin.com@GRID.LINKEDIN.COM, issueDate=1466763140395, maxDate=1467367940395, sequenceNumber=1527544, masterKeyId=62)\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token kind: RM_DELEGATION_TOKEN\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token id: [B@6e505010\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Token service: ltx1-holdemrm01.grid.linkedin.com:8032\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Tokens loaded in /grid/a/mapred/tmp/mr-azkaban1452422197532863943.token\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - 1 commands to execute.\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - effective user is: ragepati\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - effective user is: ragepati\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Command: /export/apps/azkaban/native/execute-as-user ragepati java -Dhive.exec.scratchdir=/tmp/hive-ragepati -Dgrape.root=/grid/a/tmp/grape-cache-ragepati -Dgroovy.grape.report.downloads=true -Dazkaban.flowid=help_center -Dazkaban.execid=557257 -Dazkaban.jobid=help_center_sessions -Dmapred.reduce.tasks=200 -Dmapred.job.queue.name=default -Dmapred.child.java.opts=\"-Xmx3G -server\" -Dmapred.min.split.size=536870912 -Dmapred.max.split.size=2147483648 -Dmapred.compress.map.output=true -Dmapred.output.compress=true -Dmapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec -Ddfs.block.size=536870912 -Djava.io.tmpdir=/grid/a/mapred/tmp -Dpig.usenewlogicalplan=false -Dpig.splitCombination=true -Dpig.additional.jars=*.jar:fat/*.jar -Dpig.maxCombinedSplitSize=536870912 -Djava.library.path=/export/apps/hadoop/latest/lib/native/Linux-amd64-64 -Dmapred.create.symlink=yes -Dmapreduce.reduce.input.limit=-1 -Dmapreduce.reduce.shuffle.input.buffer.percent=0.5 -Ddfs.umaskmode=022 -Dudf.import.list=com.linkedin.metrics.utils.:com.linkedin.pig.:com.linkedin.pig.date.:datafu.pig.util.:com.linkedin.metrics.:org.apache.pig.piggybank.evaluation. -Dmapreduce.job.complete.cancel.delegation.tokens=false -Dmapreduce.job.user.classpath.first=true -Dmapreduce.map.memory.mb=4096 -Dmapreduce.reduce.memory.mb=4096 -Dyarn.app.mapreduce.am.staging-dir=/user -Dhive.querylog.location=. -Dhive.mapred.supports.subdirectories=true -Dhive.aux.jars.path=file:///export/apps/hive/hive-bin_013159-0.13.1.59-1/aux/lib,file:///export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/fat/feed-0.0.49.jar,file:///export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/fat/ds-udf-pig-3.0.1.jar,file:///export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/ump-core-2.2.93.jar,file:///export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/joda-time-2.4.jar -Dhive.exec.scratchdir=/tmp/hive-ragepati -Djava.io.tmpdir=/grid/a/mapred/tmp -Djava.library.path=/export/apps/hadoop/latest/lib/native:/export/apps/hadoop/site/lib/native/ -Xms64M -Xmx2G -cp ./*:./fat/*:/export/apps/hive/hive-bin_013159-0.13.1.59-1/conf:/export/apps/pig/linkedin-pig-h2-0.11.1.60/lib/*:/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/plugins/jobtypes/hadoopJava/azkaban-jobtype-3.0.0.jar:/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/lib/azkaban-common-3.0.0.jar:/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/plugins/jobtypes/hadoopJava/azkaban-jobtype-3.0.0.jar:/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/_resources_help_center_sessions:/export/apps/hadoop/site/etc/hadoop:/export/apps/hadoop/latest/share/hadoop/common/lib/*:/export/apps/hadoop/latest/share/hadoop/common/*:/export/apps/hadoop/latest/share/hadoop/hdfs/lib/*:/export/apps/hadoop/latest/share/hadoop/hdfs/*:/export/apps/hadoop/latest/share/hadoop/yarn/lib/*:/export/apps/hadoop/latest/share/hadoop/yarn/*:/export/apps/hadoop/latest/share/hadoop/mapreduce/lib/*:/export/apps/hadoop/latest/share/hadoop/mapreduce/*:/export/apps/hadoop/site/lib/* azkaban.jobtype.HadoopJavaJobRunnerMain \n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Environment variables: {JOB_PROP_FILE=/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/help_center_sessions_props_4822820256945993012_tmp, JOB_OUTPUT_PROP_FILE=/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257/help_center_sessions_output_7794921961261630119_tmp, HADOOP_TOKEN_FILE_LOCATION=/grid/a/mapred/tmp/mr-azkaban1452422197532863943.token, KRB5CCNAME=/tmp/krb5cc__ragepati_help_center_mr__help_center__help_center_sessions__557257__ragepati, JOB_NAME=help_center_sessions}\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Working directory: /export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/executions/557257\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - Changing user: user: ragepati, uid: 20485, gid: 100\n"
      + "24-06-2016 03:12:20 PDT help_center_sessions INFO - user command starting from: java\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - SLF4J: Class path contains multiple SLF4J bindings.\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - SLF4J: Found binding in [jar:file:/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/projects/3997.1/thirdeye-hadoop-1.0.78.jar!/org/slf4j/impl/StaticLoggerBinder.class]\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - SLF4J: Found binding in [jar:file:/export/apps/azkaban/azkaban-exec-server/azkaban-exec-server-3.0.0-9/projects/3997.1/slf4j-log4j12-1.7.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - SLF4J: Found binding in [jar:file:/export/apps/hadoop/hadoop-bin_26152/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Running job help_center_sessions\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Class name com.linkedin.metrics.datafile.DataFileJob\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Found token file /grid/a/mapred/tmp/mr-azkaban1452422197532863943.token\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Security enabled is true\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Setting mapreduce.job.credentials.binary to /grid/a/mapred/tmp/mr-azkaban1452422197532863943.token\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Proxying enabled.\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Current logged in user is ragepati\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Proxied as user ragepati\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Constructor found public com.linkedin.metrics.datafile.DataFileJob(java.lang.String,azkaban.utils.Props)\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Got object com.linkedin.metrics.datafile.DataFileJob@27406a17\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Invoking method run\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Proxying enabled.\n"
      + "24-06-2016 03:12:21 PDT help_center_sessions INFO - INFO Created ivy settings file: /grid/a/tmp/grape-cache-ragepati/config/dali/dali.ivy.settings.xml for current session\n"
      + "24-06-2016 03:12:22 PDT help_center_sessions INFO - INFO Resolving artifact: {org=com.linkedin.hadoop-starter-kit, module=hello-mapreduce-azkaban, version=1.0.125, transitive=false}\n"
      + "24-06-2016 03:12:22 PDT help_center_sessions INFO - Resolving dependency: com.linkedin.hadoop-starter-kit#hello-mapreduce-azkaban;1.0.125 {default=[default]}\n"
      + "24-06-2016 03:12:22 PDT help_center_sessions INFO - Preparing to download artifact com.linkedin.hadoop-starter-kit#hello-mapreduce-azkaban;1.0.125!hello-mapreduce-azkaban.jar\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO adding datafile properties using src/help_center/sessions.properties\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO Found hadoopClass com.linkedin.hello.mapreduce.CountByCountryMapReduceJob\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO Instantiated hadoopClass com.linkedin.hello.mapreduce.CountByCountryMapReduceJob\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO Invoking run method on object of: com.linkedin.hello.mapreduce.CountByCountryMapReduceJob\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO Starting CountByCountryMapReduceJob\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO Localizing file:/user/ragepati/lib/hello-mapreduce-azkaban/xz-1.0.jar\n"
      + "24-06-2016 03:12:23 PDT help_center_sessions INFO - INFO Connecting to ResourceManager at ltx1-holdemrm01.grid.linkedin.com/10.150.1.58:8032\n"
      + "24-06-2016 03:12:24 PDT help_center_sessions INFO - WARN Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.\n"
      + "24-06-2016 03:12:24 PDT help_center_sessions INFO - INFO Total input paths to process : 7\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO number of splits:7\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO dfs.client.file-block-storage-locations.timeout is deprecated. Instead, use dfs.client.file-block-storage-locations.timeout.millis\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO io.bytes.per.checksum is deprecated. Instead, use dfs.bytes-per-checksum\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO dfs.replication.min is deprecated. Instead, use dfs.namenode.replication.min\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - Warning: Could not get charToByteConverterClass!\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO Submitting tokens for job: job_1466048666726_410150\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - WARN Cannot find class for token kind HIVE_DELEGATION_TOKEN\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - WARN Cannot find class for token kind HIVE_DELEGATION_TOKEN\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - Kind: HIVE_DELEGATION_TOKEN, Service: , Ident: 00 08 72 61 67 65 70 61 74 69 07 61 7a 6b 61 62 61 6e 3f 61 7a 6b 61 62 61 6e 2f 6c 74 78 31 2d 68 6f 6c 64 65 6d 61 7a 65 78 65 63 30 32 2e 67 72 69 64 2e 6c 69 6e 6b 65 64 69 6e 2e 63 6f 6d 40 47 52 49 44 2e 4c 49 4e 4b 45 44 49 4e 2e 43 4f 4d 8a 01 55 81 e5 5c 6f 8a 01 55 a5 f1 e0 6f 8d 04 1e 7f 8e 01 15\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO Kind: RM_DELEGATION_TOKEN, Service: ltx1-holdemrm01.grid.linkedin.com:8032, Ident: (owner=ragepati, renewer=yarn, realUser=azkaban/ltx1-holdemazexec02.grid.linkedin.com@GRID.LINKEDIN.COM, issueDate=1466763140395, maxDate=1467367940395, sequenceNumber=1527544, masterKeyId=62)\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO Kind: MR_DELEGATION_TOKEN, Service: ltx1-holdemjh01.grid.linkedin.com:10020, Ident: (owner=azkaban/ltx1-holdemazexec02.grid.linkedin.com@GRID.LINKEDIN.COM, renewer=yarn, realUser=, issueDate=1466763140314, maxDate=1467367940314, sequenceNumber=505625, masterKeyId=20)\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO Kind: HDFS_DELEGATION_TOKEN, Service: ltx1-holdemnn01.grid.linkedin.com:9000, Ident: (HDFS_DELEGATION_TOKEN token 5017233 for ragepati)\n"
      + "24-06-2016 03:12:26 PDT help_center_sessions INFO - INFO Submitted application application_1466048666726_410150\n"
      + "24-06-2016 03:12:26 PDT help_center_sessions INFO - INFO The url to track the job: http://ltx1-holdemwp01.grid.linkedin.com:8080/proxy/application_1466048666726_410150/\n"
      + "24-06-2016 03:12:26 PDT help_center_sessions INFO - INFO Running job: job_1466048666726_410150\n"
      + "24-06-2016 03:12:33 PDT help_center_sessions INFO - INFO Job job_1466048666726_410150 running in uber mode : false\n"
      + "24-06-2016 03:12:33 PDT help_center_sessions INFO - INFO  map 0% reduce 0%\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - INFO Task Id : attempt_1466048666726_410150_m_000000_0, Status : FAILED\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - Error: java.io.FileNotFoundException: Path is not a file: /data/databases/Identity/Profile/1466675602538-PT-472724050\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:70)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:56)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocationsUpdateTimes(FSNamesystem.java:1914)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocationsInt(FSNamesystem.java:1855)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocations(FSNamesystem.java:1835)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocations(FSNamesystem.java:1807)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.getBlockLocations(NameNodeRpcServer.java:552)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.getBlockLocations(ClientNamenodeProtocolServerSideTranslatorPB.java:362)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:619)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.RPC$Server.call(RPC.java:962)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2044)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2040)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat java.security.AccessController.doPrivileged(Native Method)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat javax.security.auth.Subject.doAs(Subject.java:422)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1671)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler.run(Server.java:2038\n"
      + "24-06-2016 03:13:00 PDT help_center_sessions ERROR - Job run failed!\n"
      + "24-06-2016 03:13:00 PDT help_center_sessions ERROR - java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException cause: java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException\n"
      + "24-06-2016 03:13:00 PDT help_center_sessions INFO - Finishing job help_center_sessions attempt: 0 at 1466763180242 with status FAILED";

  private String scriptLevelFailedAzkabanJobLog="28-06-2016 16:23:10 PDT job_search_trigger INFO - Starting job job_search_trigger at 1467156190329\n"
      + "28-06-2016 16:23:10 PDT job_search_trigger INFO - job JVM args: -Dazkaban.flowid=job_search -Dazkaban.execid=594359 -Dazkaban.jobid=job_search_trigger -Dmapred.reduce.tasks=${mapred.reduce.tasks} -Dmapred.job.queue.name=${mapred.job.queue.name} -Dmapred.child.java.opts=\"${mapred.child.java.opts}\" -Dmapred.min.split.size=${mapred.min.split.size} -Dmapred.max.split.size=${mapred.max.split.size} -Dmapred.compress.map.output=${mapred.compress.map.output} -Dmapred.output.compress=${mapred.output.compress} -Dmapred.map.output.compression.codec=${mapred.map.output.compression.codec} -Ddfs.block.size=${dfs.block.size} -Djava.io.tmpdir=/grid/a/mapred/tmp -Dpig.usenewlogicalplan=false -Dpig.splitCombination=true -Dpig.additional.jars=${pig.additional.jars} -Dpig.maxCombinedSplitSize=${mapred.min.split.size} -Djava.library.path=${HADOOP_HOME}/lib/native/Linux-amd64-64 -Dmapred.create.symlink=yes -Dmapreduce.reduce.input.limit=${mapreduce.reduce.input.limit} -Dmapreduce.reduce.shuffle.input.buffer.percent=0.5 -Ddfs.umaskmode=${dfs.umaskmode} -Dudf.import.list=${udf.import.list} -Dmapreduce.job.complete.cancel.delegation.tokens=${mapreduce.job.complete.cancel.delegation.tokens} -Dmapreduce.job.user.classpath.first=${mapreduce.job.user.classpath.first} -Dmapreduce.map.memory.mb=${mapreduce.map.memory.mb} -Dmapreduce.reduce.memory.mb=${mapreduce.reduce.memory.mb} -Dyarn.app.mapreduce.am.staging-dir=${yarn.tmp.dir} -Dhive.querylog.location=. -Dhive.mapred.supports.subdirectories=true -Dhive.aux.jars.path=${hive.aux.jars.path} -Dhive.exec.scratchdir=/tmp/hive-${user.to.proxy}\n"
      + "28-06-2016 16:23:10 PDT job_search_trigger INFO - Building hadoopJava job executor. \n"
      + "28-06-2016 16:23:10 PDT job_search_trigger INFO - Initiating hadoop security manager.\n"
      + "28-06-2016 16:23:10 PDT job_search_trigger INFO - Loading hadoop security manager azkaban.security.HadoopSecurityManager_H_2_0\n"
      + "28-06-2016 16:23:10 PDT job_search_trigger INFO - Need to proxy. Getting tokens.\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - INFO Last attempt: false\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Exception in thread \"main\" java.lang.reflect.UndeclaredThrowableException\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1686)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain.runMethodAsUser(HadoopJavaJobRunnerMain.java:219)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain.<init>(HadoopJavaJobRunnerMain.java:172)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain.main(HadoopJavaJobRunnerMain.java:79)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Caused by: java.lang.reflect.InvocationTargetException\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat java.lang.reflect.Method.invoke(Method.java:483)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain.runMethod(HadoopJavaJobRunnerMain.java:238)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain.access$000(HadoopJavaJobRunnerMain.java:50)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain$2.run(HadoopJavaJobRunnerMain.java:229)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat azkaban.jobtype.HadoopJavaJobRunnerMain$2.run(HadoopJavaJobRunnerMain.java:219)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat java.security.AccessController.doPrivileged(Native Method)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat javax.security.auth.Subject.doAs(Subject.java:422)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1671)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \t... 3 more\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Caused by: java.lang.RuntimeException: Backfill requires start and end date\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat com.linkedin.metrics.feeder.TriggerJob.generateDaily(TriggerJob.java:143)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat com.linkedin.metrics.feeder.TriggerJob.run(TriggerJob.java:135)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \t... 14 more\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Process completed unsuccessfully in 1 seconds.\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Got token: Kind: HIVE_DELEGATION_TOKEN, Service: , Ident: 00 05 64 61 72 79 61 07 61 7a 6b 61 62 61 6e 3f 61 7a 6b 61 62 61 6e 2f 6c 74 78 31 2d 68 6f 6c 64 65 6d 61 7a 65 78 65 63 30 31 2e 67 72 69 64 2e 6c 69 6e 6b 65 64 69 6e 2e 63 6f 6d 40 47 52 49 44 2e 4c 49 4e 4b 45 44 49 4e 2e 43 4f 4d 8a 01 55 99 52 d4 c0 8a 01 55 bd 5f 58 c0 8d 06 0b 9e 8e 01 1e\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Token kind: HIVE_DELEGATION_TOKEN\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger ERROR - Job run failed!\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger ERROR - java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException cause: java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Finishing job job_search_trigger attempt: 0 at 1467156192215 with status FAILED";

  private String azkabanLevelFailedAzkabanJobLog = "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - Starting job feature-exploration_create-index-map at 1467146727699\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - job JVM args: -Dazkaban.flowid=feature-exploration -Dazkaban.execid=593197 -Dazkaban.jobid=feature-exploration_create-index-map\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - Building spark job executor. \n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map ERROR - Failed to build job executor for job feature-exploration_create-index-mapCould not find variable substitution for variable(s) [global.jvm.args->user.to.proxy]\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map ERROR - Failed to build job type\n"
      + "azkaban.jobtype.JobTypeManagerException: Failed to build job executor for job feature-exploration_create-index-map\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map ERROR - Job run failed preparing the job.\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - Finishing job feature-exploration_create-index-map attempt: 0 at 1467146727702 with status FAILED";

  private AzkabanJobLogAnalyzer analyzedSucceededLog;
  private AzkabanJobLogAnalyzer analyzedKilledLog;
  private AzkabanJobLogAnalyzer analyzedMRLevelFailedLog;
  private AzkabanJobLogAnalyzer analyzedScriptLevelFailedLog;
  private AzkabanJobLogAnalyzer analyzedAzkabanLevelFailedLog;

  public AzkabanJobLogAnalyzerTest(){
    analyzedSucceededLog = new AzkabanJobLogAnalyzer(succeededAzkabanJobLog);
    analyzedKilledLog = new AzkabanJobLogAnalyzer(killedAzkabanJobLog);
    analyzedMRLevelFailedLog = new AzkabanJobLogAnalyzer(mrLevelFailedAzkabanJobLog);
    analyzedScriptLevelFailedLog = new AzkabanJobLogAnalyzer(scriptLevelFailedAzkabanJobLog);
    analyzedAzkabanLevelFailedLog = new AzkabanJobLogAnalyzer(azkabanLevelFailedAzkabanJobLog);
  }
  @Test
  public void getStateTest(){
    assertTrue(analyzedSucceededLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.SUCCEEDED);
    assertTrue(analyzedKilledLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.KILLED);
    assertTrue(analyzedMRLevelFailedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.MRFAIL);
    assertTrue(analyzedScriptLevelFailedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.SCRIPTFAIL);
    assertTrue(analyzedAzkabanLevelFailedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.AZKABANFAIL);
  }

  @Test
  public void getSubEventsTest(){
    assertTrue("Succeeded sub events test failed",analyzedSucceededLog.getSubEvents().isEmpty());
    assertTrue("Script level failed sub events test failed",analyzedScriptLevelFailedLog.getSubEvents().isEmpty());
    assertTrue("Azkaban level failed sub events test failed",analyzedAzkabanLevelFailedLog.getSubEvents().isEmpty());
    assertTrue(analyzedMRLevelFailedLog.getSubEvents().size() == 1);
    assertTrue(analyzedMRLevelFailedLog.getSubEvents().iterator().next().equals("job_1466048666726_410150"));
    assertTrue("Killed sub events test failed",analyzedKilledLog.getSubEvents().isEmpty());
  }

  @Test
  public void getExceptionsTest(){
    assertTrue(analyzedSucceededLog.getException() == null);
    assertTrue(analyzedKilledLog.getException() == null);
  }
}
