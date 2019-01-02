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
# Global constants
#
########################################################
# Base path for most of the quality tool reports
readonly REPORTS_BASE_PATH="target/scala-2.10/"
# Default path for Findbugs report
readonly FINDBUGS_REPORT_PATH=$REPORTS_BASE_PATH"findbugs/report.xml"

# Color coded prefixes for ERROR and SUCCESS messages
readonly SUCCESS_COLOR_PREFIX="[\033[0;32mSUCCESS\033[0m]"
readonly ERROR_COLOR_PREFIX="[\033[0;31mERROR\033[0m]"

##########################################################
# Run sbt findbugs command, parse the report and if any
# bugs are found, fail the build.
#
# Arguments:
#   None
# Returns:
#   None
##########################################################
function runFindbugs() {
  # Run findbugs
  sbt findbugs
  if [ $? -ne 0 ]; then
    echo -e "$ERROR_COLOR_PREFIX Findbugs step failed..."
    exit 1;
  fi

  # Check if there are any bugs in the Findbugs report
  if [ ! -f $FINDBUGS_REPORT_PATH ]; then
    echo -e "$ERROR_COLOR_PREFIX Findbugs report was not generated, failing the build..."
    exit 1;
  fi

  # Incorrect report. Summary does not exist hence cannot parse the report.
  summaryLine=`grep -i 'FindBugsSummary' $FINDBUGS_REPORT_PATH`
  if [ -z "$summaryLine" ]; then
    echo -e "$ERROR_COLOR_PREFIX Build failed as Findbugs summary could not be found in report..."
    exit 1;
  fi

  # Fetch bugs from the report and if any bugs are found, fail the build.
  totalBugs=`echo $summaryLine | grep -o 'total_bugs="[0-9]*'`
  totalBugs=`echo $totalBugs | awk -F'="' '{print $2}'`
  if [ $totalBugs -gt 0 ];then
    echo -e "$ERROR_COLOR_PREFIX Build failed due to "$totalBugs" Findbugs issues..."
    #exit 1;
  fi
}

########################################################
#
#                    MAIN SCRIPT
#
########################################################

echo ""
echo "************************************************************"
echo "  1. Compile Dr.Elephant code"
echo "************************************************************"
sbt clean compile
if [ $? -ne 0 ]; then
  echo -e "$ERROR_COLOR_PREFIX Compilation failed..."
  exit 1;
fi
echo -e "$SUCCESS_COLOR_PREFIX Compilation step succeeded..."

echo ""
echo "************************************************************"
echo "  2. Run Findbugs for JAVA code"
echo "************************************************************"
runFindbugs
echo -e "$SUCCESS_COLOR_PREFIX Findbugs step succeeded..."

echo ""
echo "************************************************************"
echo "  3. Run unit tests and code coverage"
echo "************************************************************"
sbt test jacoco:cover
if [ $? -ne 0 ]; then
  echo -e "$ERROR_COLOR_PREFIX Unit tests or code coverage failed..."
  exit 1;
fi

echo -e "$SUCCESS_COLOR_PREFIX Unit test and code coverage step succeeded..."
echo ""
