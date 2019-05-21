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

########################################################
#
#                  Global constants
#
########################################################
# Tab for use in sed command
readonly TAB=$'\t'

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
# configured threshold for the language. Dump CPD summary
# for changed files if checkIfCPDFailed method returns 1
# i.e. if CPD duplicates are above threshold.
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

############################################################
# Helper function to generate severity attribute found in
# checkstyle/scalastyle XML reports, based on the severity
# passed. Used to match severity in the XML report.
#
# Arguments:
#   arg1: Severity of issues (warning or error)
# Returns:
#   Severity attribute string in the form:
#     severity="arg1"
############################################################
function getSeverityStringForStyleReport() {
  severityWithQuotes='"'$1'"'
  echo 'severity='$severityWithQuotes
}

############################################################
# For the files changed in the PR and severity passed, get
# a summary of all the checkstyle or scalastyle issues in a
# printable format by extracting details from XML report.
# This function is called when warnings surpass the baseline
# or errors breach the threshold i.e. the PR introduces
# checkstyle or scalastyle errors.
#
# Arguments:
#   arg1: List of files changed in the PR
#   arg2: Report location for the tool whose report is being
#         parsed.
#   arg3: Severity of the issues for which we are dumping
#         results
# Returns:
#   None
############################################################
function getStyleIssuesForChangedFiles() {
  # Get severity attribute for match in style report.
  severityString=$(getSeverityStringForStyleReport $3)
  styleIssuesForChangedFiles=$(
    # Iterate over all the files changed in this PR.
    for changedFile in $1; do
      # sed requires filename separators to be escaped.
      changedFileEscaped=$(echo $changedFile | sed 's/\//\\\//g')

      # Extract/filter out issues based on filename and severity.
      results=`sed -n "/<file.*$changedFileEscaped.*/,/<\/file/p" $2`
      resultsFiltered=`echo "${results}" | grep $severityString`

      # Proceed only if issues are found for filename and passed severity.
      if [ ! -z "${resultsFiltered}" ]; then
        resultsFilteredCnt=`echo "${resultsFiltered}" | wc -l | xargs`
        if [ $resultsFilteredCnt -gt 0 ]; then
          echo ""
          echo -e "      - Failed checks for file $changedFile:"

          # Sanitize result from XML report. Segregate different attributes in each issue line by commas.
          finalResults=`echo "${resultsFiltered}" | sed "s/<error /  /g;s/$severityString //g;s/\/>//g;s/\" /\", /g"`
          echo "${finalResults}" | awk '{
            # Iterate over all the Checkstyle/Scalastyle issues for the changed file, filtered by severity
            numIssues = split($0, issues, "\n")
            for (issueIdx = 1; issueIdx <= numIssues; issueIdx++) {
              # Convert special encoding in XML file such as &quot and &apos
              gsub(/"/, "", issues[issueIdx]);
              gsub("\\&apos;", "'"'"'", issues[issueIdx]);
              gsub("\\&quot;", "\"", issues[issueIdx]);
              # Next 4 variables hold the attribute values for line, column, message and source attributes respectively.
              line=""
              column=""
              message=""
              source=""

              # Indicates whether message attribute is being processed.
              processingMessage = 0;

              # Extract different attributes for each issue by splitting the line by comma
              # and for each attribute, extract its value. The attributes we are interested
              # in are line, column, source and message.
              # 1. Line indicates the line at which checkstyle/scalastyle issue exists
              # 2. Column indicates the column at which checkstyle/scalastyle issue exists
              # 3. Message is explanation about the issue.
              # 4. Source is Checkstyle/Scalastyle check which led to the issue.
              numAttributes = split(issues[issueIdx], attributes, ",");
              for (attributeIdx = 1; attributeIdx <= numAttributes; attributeIdx++) {
                lineIdx = index(attributes[attributeIdx], "line=");
                if (lineIdx > 0) {
                  line = line "" substr(attributes[attributeIdx], lineIdx + 5);
                  processingMessage = 0;
                  continue;
                }
                columnIdx = index(attributes[attributeIdx], "column=");
                if (columnIdx > 0) {
                  column = column "" substr(attributes[attributeIdx], columnIdx + 7);
                  processingMessage = 0;
                  continue;
                }
                sourceIdx = index(attributes[attributeIdx], "source=");
                if (sourceIdx > 0) {
                  source = source "" substr(attributes[attributeIdx], sourceIdx + 7);
                  processingMessage = 0;
                  continue;
                }

                # Extract message from message attribute. As message can contain commas as well, continue to append to message
                # till another attribute is encountered.
                messageIdx = index(attributes[attributeIdx], "message=");
                if (messageIdx > 0) {
                  message = message "" substr(attributes[attributeIdx], messageIdx + 8);
                  processingMessage = 1;
                } else if (processingMessage == 1) {
                  message = message "," attributes[attributeIdx];
                }
              }
              # Remove dot from the end of the message.
              split(message, chars, "");
              len = length(message);
              if (chars[len] == ".") {
                message="" substr(message, 1, len - 1);
              }

              # Extract last section of source string, separated by dot.
              numSourceParts = split(source, sourceParts, ".");
              # Print style information in the desired format
              printf("\t + %s (%s) at line: %s%s\n", sourceParts[numSourceParts], message, line, ((column == "") ? "." : (" and column: " column ".")));
            }
          }'
        fi
      fi
    done
  )
  echo "${styleIssuesForChangedFiles}"
}

############################################################
# Capitalizes first character of passed string
#
# Arguments:
#   arg1: String whose first character has to be capitalized
# Returns:
#   String with first character captialized
############################################################
function capitalizeFirstCharOfStr() {
  echo $1 | awk '{
    split($0, chars, "")
    for (i = 1; i <= length($0); i++) {
      if (i == 1) {
        printf("%s", toupper(chars[i]));
      } else {
        printf("%s", chars[i]);
      }
    }
  }'
}

###################################################################
# Process checkstyle/scalastyle report after the tool has been run
# This method will find out whether report has been generated and
# how many errors/warnings exist.
# If errors exist and they are above threshold, fail the build.
# Print summary of checkstyle/scalastyle warnings for changed files
# if checkStyleToolWarnings function returns 1 i.e. if warnings are
# above baseline.
# Print summary of checkstyle/scalastyle errors for changed files
# if checkStyleToolErrors function returns 1 i.e. if errors are
# above threshold.
# If warnings exist and above baseline, warn the user and print
# top 10 issues at warning severity grouped by issue type.
# Also print top 10 issues at error severity grouped by issue
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
#   arg4: Warnings baseline for the tool whose report will be
#         processed
#   arg5: Name of the error threshold constant for the tool
#         and language
#   arg6: Name of the warning baseline constant for the tool
#         and language
#   arg7: List of files changed in the PR
# Returns:
#   None
###################################################################
function processStyleReports() {
  # Verify if style report exists for the tool whose report is being processed
  verifyStyleReportExistence $1 $2

  # Check warnings in Checkstyle/Scalastyle report
  checkStyleToolWarnings $1 $2 $4 $6 numWarnings
  result=$?
  if [ $result -gt 0 ]; then
    if [ $result -eq 1 ]; then
      # If there are warnings above baseline, find all warnings for changed files
      styleIssuesForChangedFiles=$(getStyleIssuesForChangedFiles "${7}" $2 "warning")
      fileCnt=`echo "${styleIssuesForChangedFiles}" | grep "Failed checks for file" | wc -l | xargs`
      if [ $fileCnt -gt 0 ]; then
        echo -e "$WARNING_COLOR_PREFIX Note: This PR may have introduced $1 warnings (baseline: $4)"
        echo -e "$WARNING_COLOR_PREFIX Listing $1 WARNINGS for the files changed in the PR:"
        echo "${styleIssuesForChangedFiles}"
      else
        msgToResetStyleReportWarning $1 $4 $6 $numWarnings
      fi
    fi
  fi
  echo ""

  # Check errors in Checkstyle/Scalastyle report
  checkStyleToolErrors $1 $2 $3 $5
  result=$?
  if [ $result -gt 0 ]; then
    if [ $result -eq 1 ]; then
      echo -e "$ERROR_COLOR_PREFIX Listing $1 ERRORS for the files changed in the PR:"
      styleIssuesForChangedFiles=$(getStyleIssuesForChangedFiles "${7}" $2 "error")
      echo "${styleIssuesForChangedFiles}"
      echo ""
      echo -e "$ERROR_COLOR_PREFIX $1 step failed..."
    fi
    echo ""
    exit 1;
  fi
  echo ""
}

#########################################################
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
#########################################################
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

############################################################
# Run sbt checkstyle or scalastyle command, parse the report
# and if errors are found above threshold, fail the build.
#
# Arguments:
#   arg1: Command/tool to be run (checkstyle or scalastyle)
#   arg2: Report location for the tool being run
#   arg3: Error threshold, above which build would fail, for
#         the tool being run
#   arg4: Warnings baseline for the tool being run
#   arg5: Name of the error threshold constant for the tool
#         and language
#   arg6: Name of the warning baseline constant for the tool
#         and language
#   arg7: List of files changed in the PR
# Returns:
#   None
############################################################
function runStylingTool() {
  sbt $1
  if [ $? -ne 0 ]; then
    echo -e "$ERROR_COLOR_PREFIX $1 step failed..."
    exit 1;
  fi
  if [ $1 = "scalastyle" ]; then
    preProcessScalastyleReport $SCALASTYLE_REPORT_PATH 
  fi
  processStyleReports $(capitalizeFirstCharOfStr $1) $2 $3 $4 $5 $6 "${7}"
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
echo "  4. Checkstyle for JAVA code"
echo "************************************************************"
echo -e "$INFO_COLOR_PREFIX Running Checkstyle..."
runStylingTool "checkstyle" $CHECKSTYLE_REPORT_PATH $CHECKSTYLE_ERROR_THRESHOLD $CHECKSTYLE_WARNING_BASELINE "CHECKSTYLE_ERROR_THRESHOLD" "CHECKSTYLE_WARNING_BASELINE" "${changedFilesList}"
echo -e "$SUCCESS_COLOR_PREFIX Checkstyle step succeeded..."

echo ""
echo "************************************************************"
echo "  5. Scalastyle for Scala code"
echo "************************************************************"
echo -e "$INFO_COLOR_PREFIX Running Scalastyle..."
runStylingTool "scalastyle" $SCALASTYLE_REPORT_PATH $SCALASTYLE_ERROR_THRESHOLD $SCALASTYLE_WARNING_BASELINE "SCALASTYLE_ERROR_THRESHOLD" "SCALASTYLE_WARNING_BASELINE" "${changedFilesList}"
echo -e "$SUCCESS_COLOR_PREFIX Scalastyle step succeeded..."

echo ""
echo "************************************************************"
echo "  6. Run unit tests and code coverage"
echo "************************************************************"
echo -e "$INFO_COLOR_PREFIX Running unit tests and code coverage..."
sbt test jacoco:cover
if [ $? -ne 0 ]; then
  echo -e "$ERROR_COLOR_PREFIX Unit tests or code coverage failed..."
  exit 1;
fi

echo -e "$SUCCESS_COLOR_PREFIX Unit test and code coverage step succeeded..."
echo ""
