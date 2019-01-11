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
# Script to be used for building on Travis CI
#

############################################################
# Get files chnged in this PR using git commands.
#
# Arguments:
#   None
# Returns:
#   List of files changed in the PR
############################################################
function getChangedFiles() {
  # Get commit hashes which have been added in the PR
  commitHashes=`git rev-list origin/HEAD..HEAD`
  # Extract file names changed for each commit hash
  changedFiles=$(for hash in $commitHashes; do
    fileNamesForHash=`git show --name-only --oneline $hash | awk '{if (NR > 1) print}'`
    if [ ! -z "${fileNamesForHash}" ]; then
      echo "${fileNamesForHash}"
    fi
  done)
  echo "${changedFiles}" | sort | uniq
}

##########################################################
# Check if there are duplicates in CPD report above the
# configured threshold for the language.
#
# Arguments:
#   arg1: CPD report file to be checked for duplicates
#   arg2: List of files changed in the PR
# Returns:
#   None
##########################################################
function dumpCPDSummaryForChangedFiles() {
  reportDump=`cat $1`
  for changedFile in $2; do
    fileDuplicateCnt=`echo "${reportDump}" | grep $changedFile | wc -l`
    if [ $fileDuplicateCnt -gt 0 ]; then
      echo -e "\tDuplicate info for file $changedFile:"
      echo -e "\t------------------------------------------------------------------------------------";
      echo $reportDump | awk -v filename="$changedFile" '{
        # Iterate over all the duplicates in CPD report
        numDuplicates = split($0, duplicates, "<duplication")
        for (duplicateIdx = 2; duplicateIdx <= numDuplicates; duplicateIdx++) {
          # Remove codefragment before searching for filename in this duplication instance.
          sub(/<codefragment>.*<\/codefragment>/, "", duplicates[duplicateIdx]);
          # Proceed only if filename is found.
          if (index(duplicates[duplicateIdx], filename) > 0) {
            # Sanitize the report for processing.
            sub(/<\/duplication>/, "", duplicates[duplicateIdx])
            sub(/<\/pmd-cpd>/, "", duplicates[duplicateIdx])
            gsub(/<file/, "\n<file", duplicates[duplicateIdx])
            sub(/">/, "", duplicates[duplicateIdx])
            gsub(/"\/>/, "", duplicates[duplicateIdx])
            gsub(/<file line="/, "", duplicates[duplicateIdx])
            gsub(/" /, ",", duplicates[duplicateIdx])
            gsub(/path="/, "", duplicates[duplicateIdx])
            gsub(/lines="/, "", duplicates[duplicateIdx])
            gsub(/tokens="/, "", duplicates[duplicateIdx])
            gsub(/ /, "", duplicates[duplicateIdx])
            numFiles = split(duplicates[duplicateIdx], files, "\n")
            # Extract file and duplication info and print it out.
            for (fileIdx = 1; fileIdx <= numFiles; fileIdx++) {
              split(files[fileIdx], line, ",");
              if (fileIdx == 1) {
                printf("\t  - %s lines and %s tokens are duplicate for the following:\n", line[1], line[2]);
              } else {
                printf("\t\tFilename: %s at line: %s\n", line[2], line[1]);
              }
            }
          }
        }
      }'
      echo ""
    fi
  done
}

##########################################################
# Check if there are duplicates in CPD report above the
# configured threshold for the language.
#
# Arguments:
#   arg1: Language for which CPD is run (Java or Scala)
#   arg2: Duplicates threshold for the language
#   arg3: Name of the threshold constant for the language
#   arg4: CPD report file to be checked for duplicates
#   arg5: List of files changed in the PR
# Returns:
#   None
##########################################################
function checkCPDReport() {
  checkIfCPDFailed $1 $2 $3 $4 "1"
  result=$?
  # On failure exit. If result code is 1, also dump CPD summary for changed files.
  if [ $result -gt 0 ]; then
    if [ $result -eq 1 ]; then
      echo -e "$ERROR_COLOR_PREFIX Copy Paste Detector(CPD) Summary for files changed in this PR:"
      echo ""
      dumpCPDSummaryForChangedFiles $4 "${5}"
    fi
    echo ""
    exit 1;
  fi
}

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
  echo -e "$INFO_COLOR_PREFIX Running Findbugs..."
  # Run findbugs
  sbt findbugs
  if [ $? -ne 0 ]; then
    echo -e "$ERROR_COLOR_PREFIX Findbugs step failed..."
    echo ""
    exit 1;
  fi

  # Parse and check findbugs report
  checkFindbugsReport
}

##########################################################
# Run CPD for the langauge passed, check for failures and
# move the final result to a separate file.
#
# Arguments:
#   arg1: Language for which CPD is run (Java or Scala)
#   arg2: Duplicates threshold for the language
#   arg3: Name of the threshold constant for the language
#   arg4: List of files changed in the PR
# Returns:
#   None
##########################################################
function runCPDForLanguage() {
  sbt cpd
  if [ $? -ne 0 ]; then
    echo -e "$ERROR_COLOR_PREFIX CPD for "$1" failed..."
    exit 1;
  fi
  cpd_result_file=$(getCPDReportName $CPD_REPORT_BASE_PATH $1)
  mv $CPD_REPORT_PATH $cpd_result_file
  if [ $1 = "Scala" ]; then
    removeLicenseHeaderDuplicates $cpd_result_file
  fi
  checkCPDReport $1 $2 $3 $cpd_result_file "${4}"
}

##########################################################
# Run CPD for Java and Scala one by one. For Scala, first
# change cpdLanguage setting in cpd.sbt to Language.Scala
# and then run CPD.
#
# Arguments:
#   arg1: List of files changed in the PR
# Returns:
#   None
##########################################################
function runCPD() {
  echo -e "$INFO_COLOR_PREFIX Running Copy Paste Detector(CPD) for Java..."
  runCPDForLanguage "Java" $JAVA_CPD_THRESHOLD "JAVA_CPD_THRESHOLD" "${1}"

  echo ""
  # Change language to Scala before running CPD again.
  changeCPDLanguageSetting "Language.Java" "Language.Scala"
  echo -e "$INFO_COLOR_PREFIX Running Copy Paste Detector(CPD) for Scala..."
  runCPDForLanguage "Scala" $SCALA_CPD_THRESHOLD "SCALA_CPD_THRESHOLD" "${1}"
  echo ""
}

########################################################
#
#                    MAIN SCRIPT
#
########################################################

# Import baseline/threshold numbers used across compile.sh and travis.sh
source baseline.conf
# Import common functions used across compile.sh and travis.sh
source common.sh
readonly changedFilesList=$(getChangedFiles)
echo ""
if [ ! -z "${changedFilesList}" ]; then
  echo "***********************************************************"
  echo -e "$INFO_COLOR_PREFIX Files changed (added, modified, deleted) in this PR are:"
  echo "${changedFilesList}" | awk '{
    numFiles = split($0, files, "\n")
    for (fileIdx = 1; fileIdx <= numFiles; fileIdx++) {
      printf("\t- %s\n", files[fileIdx]);
    }
  }'
fi

echo ""
echo "************************************************************"
echo "  1. Compile Dr.Elephant code"
echo "************************************************************"
echo -e "$INFO_COLOR_PREFIX Compiling code..."
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
echo "  3. Run Copy Paste Detector(CPD)"
echo "************************************************************"
runCPD "${changedFilesList}"
echo -e "$SUCCESS_COLOR_PREFIX Copy Paste Detector(CPD) step succeeded..."

echo ""
echo "************************************************************"
echo "  4. Run unit tests and code coverage"
echo "************************************************************"
echo -e "$INFO_COLOR_PREFIX Running unit tests and code coverage..."
sbt test jacoco:cover
if [ $? -ne 0 ]; then
  echo -e "$ERROR_COLOR_PREFIX Unit tests or code coverage failed..."
  exit 1;
fi

echo -e "$SUCCESS_COLOR_PREFIX Unit test and code coverage step succeeded..."
echo ""
