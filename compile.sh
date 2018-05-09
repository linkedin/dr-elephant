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

function require_programs() {
  echo "Checking for required programs..."
  missing_programs=""
  
  for program in $@; do
    if ! command -v "$program" > /dev/null; then
      missing_programs=$(printf "%s\n\t- %s" "$missing_programs" "$program")
    fi
  done 

  if [ ! -z "$missing_programs" ]; then
    echo "[ERROR] The following programs are required and are missing: $missing_programs"
    exit 1
  else
    echo "[SUCCESS] Program requirement is fulfilled!"
  fi
}

require_programs zip unzip

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


project_root=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${project_root}

cd ${project_root}


#if npm is installed, install bower,ember-cli and other components for new UI

if hash npm 2>/dev/null; then
  echo "############################################################################"
  echo "npm installation found, we'll compile with the new user interface"
  echo "############################################################################"
  set -x
  sleep 3
  ember_assets=${project_root}/public/assets
  ember_resources_dir=${ember_assets}/ember
  ember_web_directory=${project_root}/web

  # cd to the ember directory
  cd ${ember_web_directory}

  npm install
  node_modules/bower/bin/bower install
  node_modules/ember-cli/bin/ember build --prod
  rm -r ${ember_resources_dir} 2> /dev/null
  mkdir ${ember_resources_dir}
  cp dist/assets/dr-elephant.css ${ember_resources_dir}/
  cp dist/assets/dr-elephant.js ${ember_resources_dir}/
  cp dist/assets/vendor.js ${ember_resources_dir}/
  cp dist/assets/vendor.css ${ember_resources_dir}/
  cp -r dist/fonts ${ember_assets}/
  cd ${project_root}
else
  echo "############################################################################"
  echo "npm installation not found. Please install npm in order to compile with new user interface"
  echo "############################################################################"
  sleep 3
fi

trap "exit" SIGINT SIGTERM

start_script=${project_root}/scripts/start.sh
stop_script=${project_root}/scripts/stop.sh
app_conf=${project_root}/app-conf
pso_dir=${project_root}/scripts/pso

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

mkdir ${DIST_NAME}/scripts/

cp -r $pso_dir ${DIST_NAME}/scripts/

zip -r ${DIST_NAME}.zip ${DIST_NAME}

mv ${DIST_NAME}.zip ${project_root}/dist/
