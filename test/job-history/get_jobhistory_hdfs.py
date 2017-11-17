#!/bin/env python2
import argparse
import base64
import lxml.etree as etree
import hashlib
import operator
import os
import json
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import types


HDFS_CMD = "hdfs"
NUMBER_TYPES = (types.IntType, types.LongType, types.FloatType, types.ComplexType)
STRING_TYPES = (types.StringType, types.UnicodeType)


def _make_set(per_line_items):
    """Make a set out of a string of arguments.

    @param str per_line_items: A newline separated string.
    @rtype: set[str]
    """
    ret = set()
    for item in per_line_items.split("\n"):
        if item == "":
            continue
        ret.add(item)
    return ret


SPARK_SAFE_PARENTS = _make_set("""
App ID
Data Read Method
Event
Executor ID
Java Home
Java Version
Locality
Name
Reason
Result
Scala Version
Scope
Spark Version
Task Type
awt.toolkit
file.encoding
file.encoding.pkg
file.separator
java.awt.graphicsenv
java.awt.printerjob
java.class.version
java.endorsed.dirs
java.home
java.io.tmpdir
java.library.path
java.runtime.name
java.runtime.version
java.specification.name
java.specification.vendor
java.specification.version
java.vendor
java.vendor.url
java.vendor.url.bug
java.version
java.vm.info
java.vm.name
java.vm.specification.name
java.vm.specification.vendor
java.vm.specification.version
java.vm.vendor
java.vm.version
line.separator
os.arch
os.name
os.version
path.separator
spark.app.id
spark.driver.memory
spark.executor.id
spark.executor.memory
spark.extraListeners
spark.hadoop.fs.file.impl
spark.hadoop.fs.hdfs.impl
spark.master
spark.metrics.conf.*.sink.pp.class
spark.metrics.conf.*.source.jvm.class
spark.rdd.scope
spark.scheduler.mode
spark.submit.deployMode
spark.ui.filters
spark.yarn.dist.files
sun.boot.class.path
sun.boot.library.path
sun.cpu.endian
sun.cpu.isalist
sun.io.unicode.encoding
sun.java.launcher
sun.jnu.encoding
sun.management.compiler
sun.nio.ch.bugLevel
sun.os.patch.level
""")
SPARK_SAFE_VALUES = _make_set("""
System Classpath
false
true
""")
MAPREDUCE_SAFE_PARENTS = _make_set("""
taskid
state
containerId
attemptId
applicationAttemptId
string
splitLocations
jobid
""")
MAPREDUCE_SAFE_VALUES = _make_set("""
AMStarted
AM_STARTED
BYTES_READ
BYTES_WRITTEN
CLEANUP_ATTEMPT_FAILED
CLEANUP_ATTEMPT_FINISHED
CLEANUP_ATTEMPT_KILLED
CLEANUP_ATTEMPT_STARTED
COMMITTED_HEAP_BYTES
COUNTERS
CPU time spent (ms)
CPU_MILLISECONDS
FAILED_SHUFFLE
FILE: Number of bytes read
FILE: Number of bytes written
FILE: Number of large read operations
FILE: Number of read operations
FILE: Number of write operations
FILE_BYTES_READ
FILE_BYTES_WRITTEN
FILE_LARGE_READ_OPS
FILE_READ_OPS
FILE_WRITE_OPS
GC time elapsed (ms)
GC_TIME_MILLIS
HDFS: Number of bytes read
HDFS: Number of bytes written
HDFS: Number of large read operations
HDFS: Number of read operations
HDFS: Number of write operations
HDFS_BYTES_READ
HDFS_BYTES_WRITTEN
HDFS_LARGE_READ_OPS
HDFS_READ_OPS
HDFS_WRITE_OPS
INITED
JOB_ERROR
JOB_FAILED
JOB_FINISHED
JOB_INFO_CHANGED
JOB_INITED
JOB_KILLED
JOB_PRIORITY_CHANGED
JOB_QUEUE_CHANGED
JOB_STATUS_CHANGED
JOB_SUBMITTED
KILLED
MAP
MAP_ATTEMPT_FAILED
MAP_ATTEMPT_FINISHED
MAP_ATTEMPT_KILLED
MAP_ATTEMPT_STARTED
MAP_COUNTERS
MAP_INPUT_RECORDS
MAP_OUTPUT_RECORDS
MB_MILLIS_MAPS
MERGED_MAP_OUTPUTS
MILLIS_MAPS
NORMALIZED_RESOURCE
NUM_KILLED_MAPS
OFF_SWITCH
OTHER_LOCAL_MAPS
PHYSICAL_MEMORY_BYTES
RECORDS_WRITTEN
REDUCE_ATTEMPT_FAILED
REDUCE_ATTEMPT_FINISHED
REDUCE_ATTEMPT_KILLED
REDUCE_ATTEMPT_STARTED
REDUCE_COUNTERS
SETUP_ATTEMPT_FAILED
SETUP_ATTEMPT_FINISHED
SETUP_ATTEMPT_KILLED
SETUP_ATTEMPT_STARTED
SLOTS_MILLIS_MAPS
SPECULATIVE
SPILLED_RECORDS
SPLIT_RAW_BYTES
SUCCEEDED
TASK_FAILED
TASK_FINISHED
TASK_STARTED
TASK_UPDATED
TOTAL_COUNTERS
TOTAL_LAUNCHED_MAPS
VCORES_MILLIS_MAPS
VIRGIN
VIRTUAL_MEMORY_BYTES
Bytes Read
Bytes Written
Failed Shuffles
File System Counters
Input split bytes
Killed map tasks
Launched map tasks
Map input records
Map output records
Map-Reduce Framework
Merged Map outputs
Other local map tasks
Physical memory (bytes) snapshot
Spilled Records
Total committed heap usage (bytes)
Total megabyte-seconds taken by all map tasks
Total time spent by all map tasks (ms)
Total time spent by all maps in occupied slots (ms)
Total vcore-seconds taken by all map tasks
Virtual memory (bytes) snapshot
org.apache.hadoop.examples.RandomWriter$Counters
string
EventType
Event
JhCounterGroup
JhCounter
JhCounters
JobFinished
JobInfoChange
JobInited
JobPriorityChange
JobQueueChange
JobStatusChanged
JobSubmitted
JobUnsuccessfulCompletion
MapAttemptFinished
ReduceAttemptFinished
TaskAttemptFinished
TaskAttemptStarted
TaskAttemptUnsuccessfulCompletion
TaskFailed
TaskFinished
TaskStarted
TaskUpdated
acls
applicationAttemptId
attemptId
avataar
clockSplits
containerId
counters
counts
cpuUsages
diagnostics
displayName
error
event
failedDueToAttempt
failedMaps
failedReduces
finishTime
finishedMaps
finishedReduces
groups
hostname
httpPort
jobConfPath
jobName
jobQueueName
jobStatus
jobid
launchTime
locality
mapCounters
mapFinishTime
name
nodeManagerHost
nodeManagerHttpPort
nodeManagerPort
org.apache.hadoop.mapreduce.FileSystemCounter
org.apache.hadoop.mapreduce.JobCounter
org.apache.hadoop.mapreduce.TaskCounter
org.apache.hadoop.mapreduce.jobhistory
org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter
org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter
physMemKbytes
port
priority
rackname
reduceCounters
shuffleFinishTime
shufflePort
sortFinishTime
splitLocations
startTime
state
status
submitTime
successfulAttemptId
taskStatus
taskType
taskid
totalCounters
totalMaps
totalReduces
trackerName
type
uberized
userName
vMemKbytes
value
workflowAdjacencies
workflowId
workflowName
workflowNodeName
workflowTags
array
boolean
enum
int
long
map
record
default
null
""")
# Add a few that have extra trailing spaces that may be confusingly removed by IDEs.
MAPREDUCE_SAFE_VALUES.add("File Input Format Counters ")
MAPREDUCE_SAFE_VALUES.add("File Output Format Counters ")
MAPREDUCE_SAFE_VALUES.add("Job Counters ")
XML_CONF_SAFE_KEYS = _make_set("""
dfs.datanode.shared.file.descriptor.paths
dfs.domain.socket.path
hadoop.security.random.device.file.path
yarn.application.classpath
yarn.resourcemanager.ha.automatic-failover.zk-base-path
yarn.resourcemanager.zk-state-store.parent-path
yarn.timeline-service.leveldb-timeline-store.path
ha.zookeeper.acl
ha.zookeeper.parent-znode
hadoop.common.configuration.version
hadoop.http.authentication.type
hadoop.http.filter.initializers
hadoop.rpc.socket.factory.class.default
hadoop.security.crypto.codec.classes.aes.ctr.nopadding
hadoop.ssl.keystores.factory.class
hadoop.kerberos.kinit.command
io.compression.codec.bzip2.library
io.seqfile.local.dir
io.serializations
map.sort.class
mapreduce.job.inputformat.class
mapreduce.job.map.class
mapreduce.job.map.output.collector.class
mapreduce.job.output.key.class
mapreduce.job.output.value.class
mapreduce.job.outputformat.class
mapreduce.job.reduce.class
mapreduce.job.reduce.shuffle.consumer.plugin.class
mapreduce.jobhistory.recovery.store.class
mapreduce.local.clientfactory.class.name
net.topology.impl
net.topology.node.switch.mapping.impl
net.topology.script.file.name
nfs.dump.dir
nfs.exports.allowed.hosts
rpc.engine.org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB
rpc.engine.org.apache.hadoop.ipc.ProtocolMetaInfoPB
rpc.engine.org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB
yarn.ipc.rpc.class
yarn.nodemanager.aux-services.mapreduce_shuffle.class
yarn.nodemanager.aux-services.spark_shuffle.class
yarn.nodemanager.container-executor.class
yarn.nodemanager.linux-container-executor.resources-handler.class
yarn.resourcemanager.configuration.provider-class
yarn.resourcemanager.scheduler.class
yarn.resourcemanager.store.class
yarn.timeline-service.store-class
mapreduce.framework.name
yarn.app.mapreduce.am.command-opts
yarn.app.mapreduce.am.staging-dir
yarn.client.failover-proxy-provider
yarn.http.policy
yarn.nodemanager.admin-env
yarn.nodemanager.aux-services
yarn.nodemanager.docker-container-executor.exec-name
yarn.nodemanager.env-whitelist
yarn.nodemanager.linux-container-executor.cgroups.hierarchy
yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user
yarn.nodemanager.linux-container-executor.nonsecure-mode.user-pattern
yarn.nodemanager.local-dirs
yarn.nodemanager.log-dirs
yarn.nodemanager.recovery.dir
yarn.nodemanager.remote-app-log-dir-suffix
yarn.nodemanager.remote-app-log-dir
yarn.nodemanager.resourcemanager.minimum.version
yarn.resourcemanager.fs.state-store.retry-policy-spec
yarn.resourcemanager.fs.state-store.uri
yarn.resourcemanager.nodemanager.minimum.version
yarn.resourcemanager.scheduler.monitor.policies
yarn.resourcemanager.state-store.max-completed-applications
yarn.resourcemanager.zk-acl
yarn.timeline-service.http-authentication.type
hadoop.security.group.mapping
hadoop.registry.jaas.context
hadoop.registry.system.acls
hadoop.registry.zk.root
hadoop.rpc.protection
hadoop.security.authentication
hadoop.security.crypto.cipher.suite
hadoop.security.java.secure.random.algorithm
hadoop.ssl.client.conf
hadoop.ssl.enabled.protocols
hadoop.ssl.server.conf
hadoop.util.hash.type
fs.AbstractFileSystem.file.impl
fs.AbstractFileSystem.har.impl
fs.AbstractFileSystem.hdfs.impl
fs.AbstractFileSystem.viewfs.impl
fs.s3a.impl
fs.swift.impl
dfs.client.https.keystore.resource
dfs.http.policy
dfs.https.server.keystore.resource
dfs.image.compression.codec
dfs.namenode.edits.journal-plugin.qjournal
dfs.namenode.top.windows.minutes
dfs.webhdfs.user.provider.user.pattern
mapreduce.client.output.filter
mapreduce.jobhistory.http.policy
mapreduce.jobhistory.jhist.format
mapreduce.jobtracker.instrumentation
mapreduce.map.log.level
mapreduce.map.output.compress.codec
mapreduce.output.fileoutputformat.compress.codec
mapreduce.output.fileoutputformat.compress.type
mapreduce.reduce.log.level
mapreduce.reduce.shuffle.fetch.retry.enabled
mapreduce.task.profile.map.params
mapreduce.task.profile.maps
mapreduce.task.profile.params
mapreduce.task.profile.reduce.params
mapreduce.task.profile.reduces
mapreduce.tasktracker.instrumentation
mapreduce.tasktracker.taskcontroller
""")
XML_CONF_SAFE_VALUES = _make_set("""
true
false
*
none
localhost
localhost:2181
default
DEFAULT
""")
XML_CONF_SAFE_VALUES.add(" ")


def _parse_args():
    """Parse command line args."""
    usage_str = textwrap.dedent("""
    Use this program to get a snapshot of MapReduce & Spark history data to a
    local tarball. It should be called as a user that has access to the history
    directories.  Optionally, anonymization can be applied which will replace
    sensitive data (hostnames, usernames, ...) with base64'd SHA-256 hashes of
    the same data.

    Usage examples:

    # Do a basic run, getting the last 1000 MR + Spark jobs.
    get_jobhistory_hdfs.py -o /tmp/jobhistory.tgz

    # Get the last 20 jobs, with anonymization.
    get_jobhistory_hdfs.py -o /tmp/jobhistory.tgz -c 20 -a

    # Basic run, but running as the HDFS user for permission reasons.
    sudo -u hdfs ./get_jobhistory_hdfs -o /tmp/jobhistory.tgz
    """)
    parser = argparse.ArgumentParser(description=usage_str,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-c", "--count", help="How many recent jobs to retrieve", type=int,
                        default=1000)
    parser.add_argument("-d", "--mr-dir", help="History dir for mapreduce",
                        default="/user/history/done")
    parser.add_argument("-s", "--spark-dir", help="History dir for spark",
                        default="/user/spark/applicationHistory")
    parser.add_argument("-o", "--output-tarball", help="Output tarball name",
                        required=True)
    parser.add_argument("-a", "--anonymize", action="store_true", help="Anonymize output")
    return parser.parse_args()


def _get_recent_mapreduce_history_filenames(mapreduce_history_dir, count):
    """Get recent mapreduce history files.

    @param str mapreduce_history_dir: The directory in HDFS that contains mapreduce history files.
    @param int count: How many files to retrieve.
    @rtype: collections.Iterable[str]

    Returns an iterable of file paths.
    """
    # Note: the * 2 in the below is because there are two files per job.
    return sorted(_yield_mapreduce_history_files(mapreduce_history_dir))[-count * 2:]


def _yield_mapreduce_history_files(mapreduce_history_dir):
    """Get all mapreduce history dirs.

    @param str mapreduce_history_dir: The directory in HDFS that contains mapreduce history files.
    @rtype: collections.Iterable[str]

    Returns an iterable of file paths.
    """
    output = subprocess.check_output(["hdfs", "dfs", "-find", mapreduce_history_dir])
    for line in output.split("\n"):
        if len(line) == 0:
            continue
        dirname, filename = os.path.split(line)
        if not filename.startswith("job_"):
            continue
        yield line


def _get_recent_spark_history_filenames(spark_history_dir, count):
    """Get recent spark history files.

    @param str spark_history_dir: The directory in HDFS that contains spark history files.
    @param int count: How many files to retrieve.
    @rtype: collections.Iterable[str]

    Returns an iterable of file paths.
    """
    return (x[0] for x in
            sorted(_yield_spark_history_files_and_times(spark_history_dir),
                   key=operator.itemgetter(1))[-count:])


def _yield_spark_history_files_and_times(spark_history_dir):
    """Get all spark history files and their times.

    @param str spark_history_dir: The directory in HDFS that contains spark history files.
    @rtype: collections.Iterable[(str, str)]

    Returns an iterable of timestamp and file paths.
    """
    output = subprocess.check_output(["hdfs", "dfs", "-ls", spark_history_dir + "/"])
    # ls output line:
    # -rwxrwx---   3 prod spark      76762 2017-01-28 10:55 /user/spark/applicationHisto...
    line_regex = re.compile(r"^.*? (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) (.*)$")
    for line in output.split("\n"):
        if len(line) == 0:
            continue
        if line.startswith("Found"):
            continue
        m = line_regex.match(line)
        if not m:
            print >> sys.stderr, "Couldn't parse jobhistory line {}".format(line)
        else:
            yield m.group(2), m.group(1)


def _get_hdfs_file(hdfs_path, local_dir):
    """Get an hdfs file to a local path.

    @param str hdfs_path: The path on the HDFS to the file.
    @param str local_dir: The local path where the file should be saved.
    """
    print "  Retrieving {}".format(hdfs_path)
    subprocess.check_call(["hdfs", "dfs", "-get", hdfs_path, local_dir])


def _anonymize_spark_file(path):
    """Anonymize a spark job history file.

    @param str path: The local path to the file.
    """
    with open(path) as f:
        input_lines = f.readlines()
    with open(path, "w") as f:
        for input_line in input_lines:
            data = json.loads(input_line)
            data = _anonymize_spark_data(data)
            print >>f, json.dumps(data)


def _anonymize_spark_data(data, parent_info=None):
    """Anonymize a decoded json spark history object.

    @param object data: An object that may contain structure in dict or lists or be a scalar.
    @param parent_info data: A parent information object, sometimes useful for anonymization

    @rtype: object

    Returns an anonymized version of the same structure.
    """
    if isinstance(data, dict):
        # TODO: Ensure key for value Added by User still gets anonymized.
        return {k: _anonymize_spark_data(v, k) for k, v in data.iteritems()}
    elif isinstance(data, list):
        return [_anonymize_spark_data(v) for v in data]
    elif isinstance(data, NUMBER_TYPES):
        return data
    elif isinstance(data, STRING_TYPES):
        if parent_info in SPARK_SAFE_PARENTS:
            return data
        if data in SPARK_SAFE_VALUES:
            return data
        try:
            int(data)
            # Must be an integer
            return data
        except ValueError:
            # Must not have been an integer
            pass
        return _anonymize_str(data)
    else:
        raise NotImplementedError("Unknown anonymization type {}".format(data))


def _anonymize_mapreduce_file(path):
    """Anonymize a mapreduce history file.

    @param str path: The local path to the file.
    """
    with open(path) as f:
        input_lines = f.readlines()
    with open(path, "w") as f:
        print >>f, input_lines[0].rstrip()
        for input_line in input_lines[1:]:
            if input_line != "\n":
                data = json.loads(input_line)
                data = _anonymize_mapreduce_data(data)
                print >>f, json.dumps(data)
            else:
                print >>f, ""


def _anonymize_mapreduce_data(data, parent_info=None):
    """Anonymize a decoded json mapreduce job history object.

    @param object data: An object that may contain structure in dict or lists or be a scalar.
    @param parent_info data: A parent information object, sometimes useful for anonymization

    @rtype: object

    Returns an anonymized version of the same structure.
    """
    if isinstance(data, dict):
        return {k: _anonymize_mapreduce_data(v, k) for k, v in data.iteritems()}
    elif isinstance(data, list):
        return [_anonymize_mapreduce_data(v) for v in data]
    elif isinstance(data, NUMBER_TYPES):
        return data
    elif isinstance(data, STRING_TYPES):
        if parent_info in MAPREDUCE_SAFE_PARENTS:
            return data
        if data in MAPREDUCE_SAFE_VALUES:
            return data
        if _is_str_number(data):
            return data
        return _anonymize_str(data)
    elif data is None:
        return None
    else:
        raise NotImplementedError("Unknown anonymization type [{}]".format(data))


def _anonymize_xml_conf_file(path):
    """Anonymize an configuration (xml) file.

    @param str path: The local path to the file.
    """
    tree = etree.parse(path)
    root = tree.getroot()
    for sub_element in root:
        if sub_element.find("name") is None:
            continue
        name = sub_element.find("name").text
        value = sub_element.find("value").text
        if _is_str_number(value):
            continue
        if name in XML_CONF_SAFE_KEYS:
            continue
        if value in XML_CONF_SAFE_VALUES:
            continue
        new_value = _anonymize_str(value)
        sub_element.find("value").text = new_value
    tree.write(path)


def _anonymize_str(plaintext):
    """Anonymize a given string.

    @param str plaintext: The plaintext string to anonymize.
    @rtype: str
    """
    hasher = hashlib.sha256()
    hasher.update(plaintext)
    digest = hasher.digest()
    b64hashtext = base64.urlsafe_b64encode(digest)
    return b64hashtext[:len(plaintext)]


def _is_str_number(s):
    """Return if a given string is a number.

    @param str s: The string to test.
    @rtype: bool
    """
    try:
        float(s)
        return True
    except ValueError:
        pass
    if s[-1] in ('d', 'f'):
        return _is_str_number(s[:-1])
    return False


def _tar_dir(local_dir, output_tarball):
    """Compress a dir into a tarball.

    @param str local_dir: The local directory to put into the tarball.
    @param str output_dirball: The path to the file to output.
    """
    current_dir = os.getcwd()
    output_tarball_absolute = os.path.join(current_dir, output_tarball)
    try:
        os.chdir(local_dir)
        subprocess.check_call(["tar", "czf" if output_tarball.endswith("gz") else "cf",
                               output_tarball_absolute, "."])
    finally:
        os.chdir(current_dir)
    print "Wrote output tarball {}".format(output_tarball)


def _test_create_file(path):
    """Test that a given path can be created.

    @param str path: The path to try to create.
    """
    with open(path, "w"):
        pass
    os.unlink(path)


def _main():
    """Main function, separate for testing."""
    args = _parse_args()
    # Fail fast on permission errors for creating the output tarball.
    _test_create_file(args.output_tarball)
    tmpdir = tempfile.mkdtemp()
    try:
        local_spark_dir = os.path.join(tmpdir, "spark")
        os.makedirs(local_spark_dir)
        print "Retrieving {} Spark jobs into {}".format(args.count, local_spark_dir)
        for history_file in _get_recent_spark_history_filenames(args.spark_dir, args.count):
            _get_hdfs_file(history_file, local_spark_dir)
            if args.anonymize:
                local_filename = os.path.join(local_spark_dir, os.path.basename(history_file))
                _anonymize_spark_file(local_filename)

        local_mapreduce_dir = os.path.join(tmpdir, "mapreduce")
        os.makedirs(local_mapreduce_dir)
        print "Retrieving {} MapReduce jobs into {}".format(args.count, local_spark_dir)
        for history_file in _get_recent_mapreduce_history_filenames(args.mr_dir, args.count):
            _get_hdfs_file(history_file, local_mapreduce_dir)
            if args.anonymize:
                local_filename = os.path.join(local_mapreduce_dir, os.path.basename(history_file))
                if local_filename.endswith(".jhist"):
                    _anonymize_mapreduce_file(local_filename)
                else:
                    _anonymize_xml_conf_file(local_filename)

        _tar_dir(tmpdir, args.output_tarball)
    finally:
        shutil.rmtree(tmpdir)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
