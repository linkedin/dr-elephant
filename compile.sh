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
  echo ""
  echo "Usage: ./compile.sh [config_file_path] [additional_options]"
  echo "  compile.sh takes optionally, custom configuration file path(denoted as config_file_path above) as first argument."\
      "This argument can't be at any other position."
  echo "  We can also, optionally pass, additional_options, in any order. Additional options are as under:"
  echo -e "\tcoverage: Runs Jacoco code coverage and fails the build as per configured threshold"
  echo -e "\tfindbugs: Runs Findbugs for Java code"
  echo -e "\tcpd: Runs Copy Paste Detector(CPD) for Java and Scala code"
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

############################################################
# Generate CPD report based on language in the report path.
# For Scala, also remove duplicates generated due to license
# header as they are false negatives. In the end, fail the
# build if failures are found.
#
# Arguments:
#   arg1: Language (one of Java or Scala)
#   arg2: Duplicates threshold for the language
#   arg3: Name of the threshold constant for the language
# Returns:
#   None
############################################################
function processCPDReportByLanguage() {
  cpd_result_file=$(getCPDReportName $CPD_REPORT_BASE_PATH $1)
  mv $CPD_REPORT_PATH $cpd_result_file
  if [ $1 = "Scala" ]; then
    removeLicenseHeaderDuplicates $cpd_result_file
  fi
  echo "CPD report generated at path $cpd_result_file"
  checkIfCPDFailed $1 $2 $3 $cpd_result_file "0"
  result=$?
  if [ $result -gt 0 ]; then
    if [ $result -eq 2 ]; then
      echo ""
      echo -e "$WARNING_COLOR_PREFIX Note: Make sure your local repo is up to date with the branch you want to merge to, otherwise threshold/baseline "\
          "values to be updated in baseline.conf\n\tmight be different and that can lead to CI failure..."
    fi
    echo ""
    exit 1;
  fi
}

##########################################################
# Run CPD for Java and Scala one by one. For Scala, first
# change cpdLanguage setting in cpd.sbt to Language.Scala
# and then run CPD. Ensure that specific CPD reports are
# generated for each language in the report folder.
#
# Arguments:
#   arg1: Play command OPTS
# Returns:
#   None
##########################################################
function runCPD() {
  echo "Running CPD for Java"
  play_command $1 cpd
  if [ $? -ne 0 ]; then
    exit 1;
  fi
  processCPDReportByLanguage "Java" $JAVA_CPD_THRESHOLD "JAVA_CPD_THRESHOLD"

  echo "Running CPD for Scala"
  changeCPDLanguageSetting "Language.Java" "Language.Scala"
  play_command $OPTS cpd
  if [ $? -ne 0 ]; then
    # Reset language back to Java
    changeCPDLanguageSetting "Language.Scala" "Language.Java"
    exit 1;
  fi
  processCPDReportByLanguage "Scala" $SCALA_CPD_THRESHOLD "SCALA_CPD_THRESHOLD"
  # Reset language back to Java
  changeCPDLanguageSetting "Language.Scala" "Language.Java"
}

require_programs zip unzip

# Default configurations
HADOOP_VERSION="2.3.0"
SPARK_VERSION="1.4.0"


extra_commands=""
# Indicates whether a custom configuration file is passed as first parameter.
custom_config="n"
run_CPD="n"
# Process command line arguments
while :; do
  if [ ! -z $1 ]; then
    case $1 in
      coverage)
        extra_commands=$extra_commands" jacoco:cover"
        ;;
      findbugs)
        extra_commands=$extra_commands" findbugs"
        ;;
      cpd)
        run_CPD="y"
        ;;
      *)
        # User may pass the first argument(optional) which is a path to config file
        if [[ -z $extra_commands && $custom_config = "n" ]]; then
          CONF_FILE_PATH=$1

          # User must give a valid file as argument
          if [ -f $CONF_FILE_PATH ]; then
            echo "Using config file: "$CONF_FILE_PATH
          else
            echo "error: Couldn't find a valid config file at: " $CONF_FILE_PATH
            print_usage
            exit 1
          fi

          custom_config="y"
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
        else
          echo "Invalid option: $1"
          print_usage
          exit 1;
        fi
    esac
    shift
  else
    break
  fi
done
if [ $custom_config = "n" ]; then
  echo "Using the default configuration"
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
set +x
set +v

start_script=${project_root}/scripts/start.sh
stop_script=${project_root}/scripts/stop.sh
app_conf=${project_root}/app-conf
pso_dir=${project_root}/scripts/pso

# Import baseline/threshold numbers used across compile.sh and travis.sh
source baseline.conf
# Import common functions used across compile.sh and travis.sh
source common.sh

# Run the main command alongwith the extra commands passed as arguments to compile.sh
echo "Command is: play $OPTS clean compile test $extra_commands"
play_command $OPTS clean compile test $extra_commands
if [ $? -ne 0 ]; then
  echo "Build failed..."
  exit 1;
fi

if [[ $extra_commands == *"findbugs"* ]]; then
  # Parse and check findbugs report
  checkFindbugsReport
fi

# Run CPD if passed as an argument
if [ $run_CPD = "y" ]; then
  runCPD $OPTS
fi

set -v
set -x
# Echo the value of pwd in the script so that it is clear what is being removed.
rm -rf ${project_root}/dist
mkdir dist
# Run distribution
play_command $OPTS dist

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
