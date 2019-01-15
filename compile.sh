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

########################################################
#
#                  Global constants
#
########################################################

# ******************** Constants for Checkstyle *********************
# Path for Checkstyle HTML report
readonly CHECKSTYLE_HTML_REPORT_PATH="target/checkstyle-report.html"

# ******************** Constants for Scalastyle *********************
# Path for Scalastyle HTML report
readonly SCALASTYLE_HTML_REPORT_PATH="target/scalastyle-result.html"
# Path for Scalastyle HTML report generation python script
readonly SCALASTYLE_XSL_FILE="project/checkstyle-noframes-severity-sorted-modified.xsl"
# Path for Scalastyle HTML report generation python script
readonly SCALASTYLE_HTML_REPORT_GEN_SCRIPT="project/scalastyle_xml_to_html.py"

function print_usage() {
  echo ""
  echo "Usage: ./compile.sh [config_file_path] [additional_options]"
  echo "  compile.sh takes optionally, custom configuration file path(denoted as config_file_path above) as first argument."\
      "This argument can't be at any other position."
  echo "  We can also, optionally pass, additional_options, in any order. Additional options are as under:"
  echo -e "\tcoverage: Runs Jacoco code coverage and fails the build as per configured threshold"
  echo -e "\tfindbugs: Runs Findbugs for Java code"
  echo -e "\tcpd: Runs Copy Paste Detector(CPD) for Java and Scala code"
  echo -e "\tstylechecks: Runs Checkstyle for Java and Scalastyle for Scala code"
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
    echo -e "$ERROR_COLOR_PREFIX The following programs are required and are missing: $missing_programs"
    exit 1
  else
    echo -e "$SUCCESS_COLOR_PREFIX Program requirement is fulfilled!"
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
      echo -e $(noteForUpdatingRepo)" and that can lead to CI failure..."
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
  echo -e "$INFO_COLOR_PREFIX Running CPD for Java"
  play_command $1 cpd
  if [ $? -ne 0 ]; then
    echo -e "$ERROR_COLOR_PREFIX CPD for Java failed"
    exit 1;
  fi
  processCPDReportByLanguage "Java" $JAVA_CPD_THRESHOLD "JAVA_CPD_THRESHOLD"

  echo -e "$INFO_COLOR_PREFIX Running CPD for Scala"
  changeCPDLanguageSetting "Language.Java" "Language.Scala"
  play_command $OPTS cpd
  if [ $? -ne 0 ]; then
    # Reset language back to Java
    changeCPDLanguageSetting "Language.Scala" "Language.Java"
    echo -e "$ERROR_COLOR_PREFIX CPD for Scala failed"
    exit 1;
  fi
  processCPDReportByLanguage "Scala" $SCALA_CPD_THRESHOLD "SCALA_CPD_THRESHOLD"
  # Reset language back to Java
  changeCPDLanguageSetting "Language.Scala" "Language.Java"
}

##########################################################
# Note for updating repo before updating baseline.conf
#
# Arguments:
#   None
# Returns:
#   Note for updating repo
##########################################################
function noteForUpdatingRepo {
  echo -e "$WARNING_COLOR_PREFIX Note: Make sure your local repo is up to date with the branch you want to merge to, otherwise threshold/baseline "\
        "values to be updated in baseline.conf\n\tmight be different"
}

############################################################
# Process style report based on tool for which report is
# being processed. Verifies report existence, checks for
# warning baseline, checks for error threshold breach and
# if required fail the build or print appropriate message.
#
# Arguments:
#   arg1: Indicates the tool whose report will be processed
#         (Checkstyle or Scalastyle)
#   arg2: Report location for the tool whose report is to be
#         processed
#   arg3: Error threshold, above which build would fail, for
#         the tool whose report will be processed
#   arg4: Warnings baseline for the tool whose report will be
#         processed
#   arg5: Name of the error threshold constant for the tool
#         and language
#   arg6: Name of the warning baseline constant for the tool
#         and language
# Returns:
#   None
############################################################
function processStyleReport() {
  verifyStyleReportExistence $1 $2

  # Check warnings in Checkstyle/Scalastyle report
  checkStyleToolWarnings $1 $2 $4 $6 numWarnings
  result=$?
  if [ $result -gt 0 ]; then
    if [ $result -eq 1 ]; then
      msgToResetStyleReportWarning $1 $4 $6 $numWarnings
    fi
    echo -e $(noteForUpdatingRepo)"..."
  fi
  echo ""

  # Check errors in Checkstyle/Scalastyle report
  checkStyleToolErrors $1 $2 $3 $5
  result=$?
  if [ $result -gt 0 ]; then
    if [ $result -eq 2 ]; then
      echo -e $(noteForUpdatingRepo)" and that can lead to CI failure..."
    fi
    echo ""
    exit 1;
  fi
  echo ""
}

############################################################
# Process both Checkstyle and Scalastyle XML reports. Also
# generates Scalastyle HTML report(Checkstyle HTML report is
# automatically generated by checkstyle4sbt plugin).
# Fail the build if threshold values are breached.
#
# Arguments:
#   None
# Returns:
#   None
############################################################
function processCheckstyleAndScalastyleReports() {
  echo ""
  echo -e "$INFO_COLOR_PREFIX Checking Checkstyle report..."
  echo -e "$INFO_COLOR_PREFIX Checkstyle XML report generated at path: $CHECKSTYLE_REPORT_PATH and HTML report generated at path: $CHECKSTYLE_HTML_REPORT_PATH"
  processStyleReport "Checkstyle" $CHECKSTYLE_REPORT_PATH $CHECKSTYLE_ERROR_THRESHOLD $CHECKSTYLE_WARNING_BASELINE "CHECKSTYLE_ERROR_THRESHOLD" "CHECKSTYLE_WARNING_BASELINE"

  scalastyleHtmlGenMsg=""
  preProcessScalastyleReport $SCALASTYLE_REPORT_PATH
  pythonVersion=`python --version 2>&1`
  if [ $? -ne 0 ]; then
    echo -e "$WARNING_COLOR_PREFIX Cannot generate Scalastyle HTML report as Python is unavailable. Install Python and add it in PATH"
  else
    # Generate Scalastyle HTML Report
    rm -rf $SCALASTYLE_HTML_REPORT_PATH
    echo "Using $pythonVersion"
    pip install lxml
    if [ $? -ne 0 ]; then
      echo -e "$WARNING_COLOR_PREFIX Could not install lxml module for Python. Scalastyle HTML report could not be generated"
    else
      python $SCALASTYLE_HTML_REPORT_GEN_SCRIPT $SCALASTYLE_REPORT_PATH $SCALASTYLE_XSL_FILE $SCALASTYLE_HTML_REPORT_PATH
      if [ $? -ne 0 ]; then
        echo -e "$WARNING_COLOR_PREFIX Scalastyle HTML report could not be generated"
      else
        scalastyleHtmlGenMsg=" and HTML report generated at path: $SCALASTYLE_HTML_REPORT_PATH"
      fi
    fi
  fi
  echo -e "$INFO_COLOR_PREFIX Checking Scalastyle report..."
  echo -e "$INFO_COLOR_PREFIX Scalastyle XML report generated at path: $SCALASTYLE_REPORT_PATH"$scalastyleHtmlGenMsg
  processStyleReport "Scalastyle" $SCALASTYLE_REPORT_PATH $SCALASTYLE_ERROR_THRESHOLD $SCALASTYLE_WARNING_BASELINE "SCALASTYLE_ERROR_THRESHOLD" "SCALASTYLE_WARNING_BASELINE"
}

#############################################################
# Run Checkstyle and Scalastyle and then process the report.
# Fail the build if the command fails or if threshold values
# are breached.
#
# Arguments:
#   arg1: Play command OPTS
# Returns:
#   None
#############################################################
function runStyleChecks() {
  echo -e "$INFO_COLOR_PREFIX Running Checkstyle and Scalastyle"
  play_command $1 checkstyle scalastyle
  if [ $? -ne 0 ]; then
    echo -e "$ERROR_COLOR_PREFIX Either Checkstyle or Scalastyle has failed"
    echo ""
    exit 1;
  fi
  processCheckstyleAndScalastyleReports
}

require_programs zip unzip

# Default configurations
HADOOP_VERSION="2.3.0"
SPARK_VERSION="1.4.0"


extra_commands=""
# Indicates whether a custom configuration file is passed as first parameter.
custom_config="n"
run_CPD="n"
run_StyleChecks="n"
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
      stylechecks)
        run_StyleChecks="y"
        ;;
      help)
        print_usage
        exit 0;
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

# Run Checkstyle and Scalastyle if stylechecks is passed as an argument
if [ $run_StyleChecks = "y" ]; then
  runStyleChecks $OPTS
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
