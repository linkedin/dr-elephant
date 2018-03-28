#!/usr/bin/env bash

#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

function print_usage() {
  echo "usage: ./start.sh PATH_TO_APP_CONFIG_DIR(optional, if you have already set env variable ELEPHANT_CONF_DIR)"
}

function check_config() {
  if [ -z "${!1}" ]; then
    echo "error: ${1} must be present in the config file."
    check=0
  else
    echo "${1}: " ${!1}
  fi
}

# Save project root dir
project_root=$( cd "$( dirname "${BASH_SOURCE[0]}" )/../" && pwd )

scripts_dir=$project_root/scripts
echo "Scripts directory: $scripts_dir"
export PSO_DIR_PATH=$scripts_dir/pso

# User could set an environmental variable, ELEPHANT_CONF_DIR, or pass an optional argument(config file path)
if [ -z "$1" ]; then
  if [ -z "$ELEPHANT_CONF_DIR" ]; then
      if [ -d "${project_root}/app-conf" ]; then
          ELEPHANT_CONF_DIR=$project_root/app-conf
      else
         echo "error: Couldn't find the configuration directory."
         echo "Please set env variable ELEPHANT_CONF_DIR to the configuration directory or pass the location as an argument."
         print_usage
         exit 1
      fi
  fi
  CONF_DIR="$ELEPHANT_CONF_DIR"
else
  CONF_DIR=$1
fi

# Verify and get absolute path to conf
if [ -d "$CONF_DIR" ]; then
  CONF_DIR=`cd "$CONF_DIR";pwd`
  echo "Using config dir: $CONF_DIR"
else
  echo "error: ${1} is not a directory or it does not exist. Please specify the application's configuration directory(app-conf)"
  print_usage
  exit 1
fi

# set/update env variable so Dr. run script will use this dir and load all confs into classpath
export ELEPHANT_CONF_DIR=$CONF_DIR

CONFIG_FILE=$ELEPHANT_CONF_DIR"/elephant.conf"
echo "Using config file: "$CONFIG_FILE

# User must give a valid file as argument
if [ -f $CONFIG_FILE ];
then
  echo "Reading from config file..."
else
  echo "error: Couldn't find a valid config file at: " $CONFIG_FILE
  print_usage
  exit 1
fi

source $CONFIG_FILE

# db_url, db_name ad db_user must be present in the config file
check=1
check_config db_url
check_config db_name
check_config db_user

if [ $check = 0 ];
then
  echo "error: Failed to get configs for dr.Elephant. Please check the config file."
  exit 1
fi

db_loc="jdbc:mysql://"$db_url"/"$db_name"?characterEncoding=UTF-8"

# db_password is optional. default is ""
db_password="${db_password:-""}"

#port is optional. default is 8080
port="${port:-8080}"
echo "http port: " $port

# Check for keytab_user, keytab_location and application_secret in the elephant.conf
if [ -n "${keytab_user}" ]; then
  echo "keytab_user: " $keytab_user
  OPTS+=" -Dkeytab.user=$keytab_user"
fi

if [ -n "${keytab_location}" ]; then
  echo "keytab_location: " $keytab_location
  OPTS+=" -Dkeytab.location=$keytab_location"
fi
if [ -n "${krb_conf_file}" ]; then
  echo "krb_conf_file: " $krb_conf_file
  OPTS+=" -Djava.security.krb5.conf=$krb_conf_file"
fi

if [ -n "${application_secret}" ]; then
  OPTS+=" -Dapplication.secret=$application_secret"
fi

# Enable web analytics if configured
if [ -n "${enable_analytics}" ]; then
  OPTS+=" -Denable.analytics=$enable_analytics"
fi

# Enable Dropwizard metrics if configured
if [ -n "${metrics}" ]; then
  OPTS+=" -Dmetrics=$metrics"
fi

# Enable metrics agent jar if configured. Agent publishes metrics to other apps.
if [ -n "${metrics_agent_jar}" ]; then
  OPTS+=" -J$metrics_agent_jar"
fi


# Navigate to project root
cd $project_root

# Check if Dr. Elephant already started
if [ -f RUNNING_PID ];
then
  echo "error: Dr. Elephant already started!"
  exit 1
fi

# Dr. Elephant executable not found
if [ ! -f bin/dr-elephant ];
then
  echo "error: I couldn't find any dr. Elephant executable."
  exit 1
fi

# Get hadoop version by executing 'hadoop version' and parse the result
HADOOP_VERSION=$(hadoop version | awk '{if (NR == 1) {print $2;}}')
if [[ $HADOOP_VERSION == 1* ]];
then
  echo "This is hadoop1.x grid. Switch to hadoop2 if you want to use Dr. Elephant"
elif [[ $HADOOP_VERSION == 2* ]];
then
  JAVA_LIB_PATH=$HADOOP_HOME"/lib/native"
  echo "This is hadoop2.x grid. Adding Java library to path: "$JAVA_LIB_PATH
else
  echo "error: Hadoop isn't properly set on this machine. Could you verify cmd 'hadoop version'? "
  exit 1
fi

OPTS+=" $jvm_args -Djava.library.path=$JAVA_LIB_PATH"
OPTS+=" -Dhttp.port=$port"
OPTS+=" -Ddb.default.url=$db_loc -Ddb.default.user=$db_user -Ddb.default.password=$db_password"

# set Java related options (e.g. -Xms1024m -Xmx1024m)
export JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError"

# Start Dr. Elaphant
echo "Starting Dr. Elephant ...."
nohup ./bin/dr-elephant ${OPTS} > $project_root/dr.log 2>&1 &

sleep 2

# If Dr. Elephant starts successfully, Play should create a file 'RUNNING_PID' under project root
if [ -f RUNNING_PID ];
then
  echo "Dr. Elephant started."
else
  echo "error: Failed to start Dr. Elephant. Please check if this is a valid dr.E executable or logs under 'logs' directory."
  exit 1
fi
