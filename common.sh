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

#
# This script contains common functions and constants which will be used by both
# compile.sh and travis.sh while running different tools.
#

########################################################
#
#                  Global constants
#
########################################################
# Base path for most of the quality tool reports
readonly REPORTS_BASE_PATH="target/scala-2.10/"

# ******************** Constants for Findbugs *********************
# Default path for Findbugs report
readonly FINDBUGS_REPORT_PATH=$REPORTS_BASE_PATH"findbugs/report.xml"

# ************* Constants for Copy Paste Detector(CPD) *************
# CPD report resides in this path
readonly CPD_REPORT_BASE_PATH=$REPORTS_BASE_PATH"cpd/"
# Default path for CPD report
readonly CPD_REPORT_PATH=$CPD_REPORT_BASE_PATH"cpd.xml"

# ************************ Other constants **************************
# Color coded prefixes for ERROR, WARNING, INFO and SUCCESS messages
readonly ERROR_COLOR_PREFIX="[\033[0;31mERROR\033[0m]"
readonly WARNING_COLOR_PREFIX="[\033[0;33mWARNING\033[0m]"
readonly INFO_COLOR_PREFIX="[\033[0;36mINFO\033[0m]"
readonly SUCCESS_COLOR_PREFIX="[\033[0;32mSUCCESS\033[0m]"

##########################################################
# Get CPD report name based on language.
#
# Arguments:
#   arg1: Report location
#   arg2: Language for which CPD report willbe generated
#         (Java or Scala)
# Returns:
#   File name where CPD report will be written to.
##########################################################
function getCPDReportName() {
  echo $1"cpd-"$2".xml"
}

##########################################################
# Check if there is a failure due to duplicates in CPD
# report above the configured threshold for the language.
#
# Arguments:
#   arg1: Language for which CPD is run (Java or Scala)
#   arg2: Duplicates threshold for the language
#   arg3: Name of the threshold constant for the language
#   arg4: CPD report file which contains duplicates
#   arg5: Flag which indicates whether to dump CPD report
#         Report will be dumped if value of argument is 1
# Returns:
#   0: Success
#   1: Failure due to threshold
#   2: Failure due to threshold variables not updated
##########################################################
function checkIfCPDFailed() {
  duplicates=`grep "<duplication lines=" $4 | wc -l | xargs`
  msg="CPD $1 Report has $duplicates duplicates"
  if [ $duplicates -gt $2 ]; then
    if [ $5 -eq 1 ]; then
      echo -e "$ERROR_COLOR_PREFIX Dumping results on failure..."
      echo "***************** Code duplication report ********************"
      cat $4
      echo "**************************************************************"
    fi
    echo -e "$ERROR_COLOR_PREFIX CPD for $1 code failed as $duplicates duplicates found (threshold: $2)"
    return 1;
  elif [ $duplicates -eq 0 ]; then
    if [ $duplicates -lt $2 ]; then
      echo -e "$ERROR_COLOR_PREFIX $msg and you have fixed some CPD issues in this PR which is great! But you forgot to update the threshold, hence failing the build."
      echo -e "\tPlease modify $3 variable to $duplicates in baseline.conf to ensure that the new threshold takes effect for subsequent builds."
      return 2;
    else
      echo -e "$SUCCESS_COLOR_PREFIX $msg"
    fi
  elif [ $duplicates -eq $2 ]; then
    echo -e "$WARNING_COLOR_PREFIX $msg but it is within threshold hence not failing the build"
  else
    echo -e "$ERROR_COLOR_PREFIX $msg and you have fixed some CPD issues in this PR which is great! But you forgot to update the threshold, hence failing the build."
    echo -e "\tPlease modify $3 variable to $duplicates in baseline.conf to ensure that the new threshold takes effect for subsequent builds."
    return 2;
  fi
  return 0;
}

##########################################################
# Parse the findbugs report and if any bugs are found,
# fail the build.
#
# Arguments:
#   None
# Returns:
#   None
##########################################################
function checkFindbugsReport() {
  # Check if there are any bugs in the Findbugs report
  if [ ! -f $FINDBUGS_REPORT_PATH ]; then
    echo -e "$ERROR_COLOR_PREFIX Findbugs report was not generated, failing the build..."
    echo ""
    exit 1;
  fi

  # Incorrect report. Summary does not exist hence cannot parse the report.
  summaryLine=`grep -i 'FindBugsSummary' $FINDBUGS_REPORT_PATH`
  if [ -z "$summaryLine" ]; then
    echo -e "$ERROR_COLOR_PREFIX Build failed as Findbugs summary could not be found in report..."
    echo ""
    exit 1;
  fi

  # Fetch bugs from the report and if any bugs are found, fail the build.
  totalBugs=`echo $summaryLine | grep -o 'total_bugs="[0-9]*'`
  totalBugs=`echo $totalBugs | awk -F'="' '{print $2}'`
  if [ $totalBugs -gt 0 ];then
    echo -e "$ERROR_COLOR_PREFIX Build failed due to "$totalBugs" Findbugs issues..."
    exit 1;
  fi
  echo -e "$INFO_COLOR_PREFIX Findbugs report generated at path $FINDBUGS_REPORT_PATH"
}

##########################################################
# Scala CPD reports count even Apache license headers as
# duplicates. Remove them from the CPD report.
#
# Arguments:
#   arg1: CPD report file to be checked for duplicates
# Returns:
#   None
##########################################################
function removeLicenseHeaderDuplicates() {
  mv $1 $1".bak"
  # For each duplication start tag match, do the following
  awk '{ p = 1 } /<duplication /{
    tag = $0;
    while (getline > 0) {
      tag = tag ORS $0;
      # Remove section which contains the License
      if (/Licensed under the Apache License/) {
        p = 0;
      }
      # Break out of loop if duplication end tag matches
      if (/<\/duplication>/) {
        break;
      }
    }
    $0 = tag
  } p' $1".bak" > $1
  rm -rf $1".bak"
}

##########################################################
# Change cpdLanguage setting in cpd.sbt from the passed
# language in first argument to language in second
# argument.
# Note: For consistency across platforms not using sed's
# -i option and instead redirecting output and moving
# files.
#
# Arguments:
#   arg1: Language setting changed from
#   arg2: Language setting changed to
# Returns:
#   None
##########################################################
function changeCPDLanguageSetting() {
  sed "s/$1/$2/g" cpd.sbt > cpd.sbt.bak
  mv cpd.sbt.bak cpd.sbt
}
