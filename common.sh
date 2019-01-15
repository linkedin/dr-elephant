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

# ******************* Constants for Checkstyle *********************
# Path for Checkstyle report
readonly CHECKSTYLE_REPORT_PATH="target/checkstyle-report.xml"

# ******************* Constants for Scalastyle *********************
# Path for Scalastyle report
readonly SCALASTYLE_REPORT_PATH="target/scalastyle-result.xml"

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
      handleSettingThresholdVariable "CPD $1" $duplicates "duplicates" $3
      return 2;
    else
      echo -e "$SUCCESS_COLOR_PREFIX $msg"
    fi
  elif [ $duplicates -eq $2 ]; then
    echo -e "$WARNING_COLOR_PREFIX $msg but it is within threshold hence not failing the build"
  else
    handleSettingThresholdVariable "CPD $1" $duplicates "duplicates" $3
    return 2;
  fi
  return 0;
}

############################################################
# Prints message to reset style report warnings variable in
# baseline.conf, if number of warnings are above baseline.
#
# Arguments:
#   arg1: Indicates the tool whose report is being parsed
#         (Checkstyle or Scalastyle)
#   arg2: Warnings baseline value for the tool
#   arg3: Warnings baseline variable for the tool
#   arg4: Number of warnings which will be used to set the
#         variable passed as arg3
# Returns:
#   None
############################################################
function msgToResetStyleReportWarning() {
  echo -e "$WARNING_COLOR_PREFIX $1 warnings are above baseline: $2. If your changes have not introduced these warnings or they can't be fixed,"\
      "consider resetting the variable $3 to $4 in baseline.conf"
}

############################################################
# Verify if style report for the tool being run exists.
#
# Arguments:
#   arg1: Indicates the tool whose report is being checked
#         (Checkstyle or Scalastyle)
#   arg2: Report location for the tool
# Returns:
#   None
############################################################
function verifyStyleReportExistence() {
  # Check if report exists
  if [ ! -f $2 ]; then
    echo -e "$ERROR_COLOR_PREFIX $1 report was not generated, failing the build..."
    exit 1;
  fi
}

############################################################
# For the severity passed, group checkstyle or scalastyle
# issues by issue type and dump top 10 results.
#
# Arguments:
#   arg1: Indicates the tool whose report is being parsed
#         (Checkstyle or Scalastyle)
#   arg2: Report location for the tool whose report is being
#         parsed.
#   arg3: Severity of the issues for which we are dumping
#         results
#   arg4: Number of issues at the severity passed as arg3
# Returns:
#   None
############################################################
function dumpTop10StyleIssueTypes() {
  # Convert passed severity to format in the report
  severityWithQuotes='"'$3'"'
  severityString='severity='$severityWithQuotes
  # Extract issue type for the passed severity, find the top 10 results based
  # on occurences of each issue type and dump them in the format below:
  # issue-type: occurences of this issue type
  result=`grep $severityString $2 | grep -o 'source="\S*' | sort | uniq -c |\
      sort -r -n -k1 | head -10 | sed 's/source="//g;s/"\/>//g'| awk -F" " '{printf("\t%s: %s\n", $2,$1);}'`
  resultCount=`echo "${result}" | wc -l | xargs`
  echo -e "$WARNING_COLOR_PREFIX $4 $1 "$3"s detected. Top $resultCount issue types with their counts are as under:"
  echo "${result}"
}

##########################################################
# This function is called when build has to be failed
# because developer has fixed CPD or Checkstyle or
# Scalastyle issues but not updated the corresponding
# threshold variable in this script.
#
# Arguments:
#   arg1: Report description
#   arg2: Issue count
#   arg3: Issue description
#   arg4: Name of variable to be updated
# Returns:
#   None
##########################################################
function handleSettingThresholdVariable() {
  msg="$1 Report has $2 $3"
  color=$ERROR_COLOR_PREFIX
  failTheBuildMsg=", hence failing the build.\n\tPlease modify"
  thresholdOrBaseline="threshold"
  if [ $3 = "warnings" ]; then
    color=$WARNING_COLOR_PREFIX
    failTheBuildMsg=".\n\tYou can modify"
    thresholdOrBaseline="baseline"
  fi
  echo -e "$color $msg and you have fixed some of them as part of this change which is great! But you forgot to update the $thresholdOrBaseline$failTheBuildMsg"\
      "$4 variable to $2 in baseline.conf to ensure that the new $thresholdOrBaseline takes effect for subsequent builds."
}

##################################################################
# Check if there are warnings in the report for the tool whose
# report is being processed. If warnings exist, dump top 10 issues
# grouped by issue type. Return an integer indicating whether
# number of warnings are 0, above or below baseline.
# Also print messages if baseline variable has to be updated in
# baseline.conf. Number of warnings are also returned by setting
# an argument passed to the function.
#
# Arguments:
#   arg1: Indicates the tool whose report will be processed
#         (Checkstyle or Scalastyle)
#   arg2: Report location for the tool whose report is to be
#         processed
#   arg3: Warnings baseline for the tool whose report will be
#         processed
#   arg4: Name of the warning baseline constant for the tool
#   arg5: Argument which will be set equal to number of
#         warnings found in the report.
# Returns:
#   0: Success
#   1: Warnings above baseline
#   2: Warnings fixed but baseline variable not updated
################################################################
function checkStyleToolWarnings() {
  # Local variable which references to arg5.
  local  __numWarnings=$5
  # Check if there are any warnings in the Checkstyle or Scalastyle report
  local styleWarnings=`grep 'severity="warning"' $2 | wc -l | xargs`
  # Effectively sets number of warnings to arg5
  eval $__numWarnings="'$styleWarnings'"
  if [ $styleWarnings -gt 0 ]; then
    dumpTop10StyleIssueTypes $1 $2 "warning" $styleWarnings
    # Return number of warnings only if over baseline
    if [ $styleWarnings -gt $3 ]; then
      return 1;
    elif [ $styleWarnings -lt $3 ]; then
      handleSettingThresholdVariable $1 $styleWarnings "warnings" $4
      return 2;
    fi
  else
    echo -e "$SUCCESS_COLOR_PREFIX $1 Report has no warnings..."
  fi
  return 0;
}

##################################################################
# Process checkstyle/scalastyle report after the tool has been run
# This method will find how many errors exist.
# If errors exist and they are above threshold, fail the build.
# Fail the build even if errors have been fixed but threshold
# variable has not been updated in baseline.conf
# Print top 10 issues at error severity grouped by issue
# type if errors are equal to threshold (for informational
# purposes)
#
# Arguments:
#   arg1: Indicates the tool whose report will be processed
#         (Checkstyle or Scalastyle)
#   arg2: Report location for the tool whose report is to be
#         processed
#   arg3: Error threshold, above which build would fail, for
#         the tool whose report will be processed
#   arg4: Name of the error threshold constant for the tool
#
# Returns:
#   0: Success
#   1: Failure due to errors above threshold
#   2: Failure due to errors fixed but threshold variable not
#      updated.
##################################################################
function checkStyleToolErrors() {
  # Check if there are any errors in the Checkstyle or Scalastyle report and fail the build, if above threshold
  styleErrors=`grep 'severity="error"' $2 | wc -l | xargs`
  if [ $styleErrors -gt $3 ]; then
    echo -e "$ERROR_COLOR_PREFIX Build failed as the code change has introduced $1 ERRORS. $styleErrors found (threshold: $3)"
    return 1;
  fi

  # Print top 10 checkstyle/scalastyle error categories if number of errors within threshold
  if [ $styleErrors -gt 0 ]; then
    if [ $styleErrors -gt $3 ]; then
      echo -e "$ERROR_COLOR_PREFIX Build failed as this code change has introduced $1 ERRORS. $styleErrors found (threshold: $3)"
      return 1;
    elif [ $styleErrors -eq $3 ]; then
      dumpTop10StyleIssueTypes $1 $2 "error" $styleErrors
      echo -e "$WARNING_COLOR_PREFIX Note: The code change may not have introduced $1 errors as count is within threshold. Not failing"\
          "the build."
      return 0;
    else
      handleSettingThresholdVariable $1 $styleErrors "errors" $4
      return 2;
    fi
  else
    if [ $3 -gt 0 ]; then
      handleSettingThresholdVariable $1 $styleErrors "errors" $4
      return 2;
    else
      echo ""
      echo -e "$SUCCESS_COLOR_PREFIX $1 Report has no errors..."
      return 0;
    fi
  fi
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

############################################################
# Generate a final scalastyle report by removing duplicate
# errors and sorting the results within a file by line
# number.
#
# Arguments:
#   arg1: Report location for the tool being run
# Returns:
#   None
############################################################
function preProcessScalastyleReport() {
  # Flag to indicate whether we are processing file tag i.e. we have encountered file begin tag but not the file end tag
  filetag=0
  currentLineNum=0
  currentErrorTag=""
  count=0
  while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ $line == *"<file "* ]]; then
      # On start file tag, copy the line and set the filetag flag
      echo -e $line >> $1.bak
      filetag=1
    elif [[ $line == *"</file>"* ]]; then
      # On end file tag, sort and find unique lines in tmpResult file(contains errors for a file).
      # This is done to avoid duplicates
      sortedResults=`cat tmpResult | sort -n -k 1 | uniq`
      # Remove the line number prepended in tmpResult used for sorting errors by line number
      finalResults=`echo "${sortedResults}" | sed 's/^[0-9]* / /g'`
      # Copy errors for a file in sorted order and after removing duplicates
      echo "${finalResults}" >> $1.bak
      rm -rf tmpResult
      # Copy file end tag as well
      echo -e $line >> $1.bak
      filetag=0
    elif [ $filetag -eq 1 ]; then
      # We are processing errors inside a file
      # Fetch line number from the corresponding attribute and prepend the line with line number
      # This is done to ensure sorting of errors within a file by line number and removing duplicates,
      # if any. Store this result in a tmpResult file
      lineAttribute=`echo "$line" | sed -n 's/.* line="\([0-9.]*\).*/\1/p'`
      if [[ $line == *"<error "* ]]; then
        if [[ $line == *"/>" ]]; then
          echo -e $lineAttribute" "$line >> tmpResult
        else
          currentLineNum=$lineAttribute
          currentErrorTag=$line
        fi
      elif [[ $line == *"/>" ]]; then
         # Processing error tag. Encountered end of tag.
         lineWithoutSpaces=`echo $line | sed 's/^[ ]*//g'`
         echo -e "$currentLineNum $currentErrorTag $lineWithoutSpaces" >> tmpResult
      fi
    else
      # Not inside file tag. Copy line as is.
      echo -e $line >> $1.bak
    fi
  done< $1
  # Move the .bak file to the report file
  mv $1.bak $1
}
