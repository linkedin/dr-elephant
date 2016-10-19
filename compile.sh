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

function print_usage(){
  echo "usage: ./compile.sh PATH_TO_CONFIG_FILE(optional)"
}

function play_command() {
  if type activator 2>/dev/null; then
    activator "$@"
  else
    play "$@"
  fi
}

# Default configurations
HADOOP_VERSION="2.3.0"
SPARK_VERSION="1.4.0"

# User should pass an optional argument which is a path to config file
if [ -z "$1" ];
then
  echo "Using the default configuration"
else
  CONF_FILE_PATH=$1
  echo "Using config file: "$CONF_FILE_PATH

  # User must give a valid file as argument
  if [ -f $CONF_FILE_PATH ];
  then
    echo "Reading from config file..."
  else
    echo "error: Couldn't find a valid config file at: " $CONF_FILE_PATH
    print_usage
    exit 1
  fi

  source $CONF_FILE_PATH

  # Fetch the Hadoop version
  if [ -n "${hadoop_version}" ]; then
    HADOOP_VERSION=${hadoop_version}
  fi

  # Fetch the Spark version
  if [ -n "${spark_version}" ]; then
    SPARK_VERSION=${spark_version}
  fi

  # Fetch other play opts
  if [ -n "${play_opts}" ]; then
    PLAY_OPTS=${play_opts}
  fi
fi

echo "Hadoop Version : $HADOOP_VERSION"
echo "Spark Version  : $SPARK_VERSION"
echo "Other opts set : $PLAY_OPTS"

OPTS+=" -Dhadoopversion=$HADOOP_VERSION"
OPTS+=" -Dsparkversion=$SPARK_VERSION"
OPTS+=" $PLAY_OPTS"

set -x
trap "exit" SIGINT SIGTERM

project_root=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${project_root}

start_script=${project_root}/scripts/start.sh
stop_script=${project_root}/scripts/stop.sh
app_conf=${project_root}/app-conf

# Echo the value of pwd in the script so that it is clear what is being removed.
rm -rf ${project_root}/dist
mkdir dist

play_command $OPTS clean test compile dist

cd target/universal

ZIP_NAME=`/bin/ls *.zip`
unzip ${ZIP_NAME}
rm ${ZIP_NAME}
DIST_NAME=${ZIP_NAME%.zip}

chmod +x ${DIST_NAME}/bin/dr-elephant

# Append hadoop classpath and the ELEPHANT_CONF_DIR to the Classpath
sed -i.bak $'/declare -r app_classpath/s/.$/:`hadoop classpath`:${ELEPHANT_CONF_DIR}"/' ${DIST_NAME}/bin/dr-elephant

cp $start_script ${DIST_NAME}/bin/

cp $stop_script ${DIST_NAME}/bin/

cp -r $app_conf ${DIST_NAME}

zip -r ${DIST_NAME}.zip ${DIST_NAME}

mv ${DIST_NAME}.zip ${project_root}/dist/
