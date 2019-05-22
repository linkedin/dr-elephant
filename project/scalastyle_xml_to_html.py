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
# This script is meant to convert Scalastyle XML report to Scalastyle HTML report using
# XSLT transformations. The XSL file is typically the same as the one used to convert
# Checkstyle XML report to HTML report.
#

import lxml.etree as ET
import os
import sys
import traceback
import os.path
from os import path

# Takes 3 arguments: Scalastyle XML report, XSL file name and HTML report name to be generated
if len(sys.argv) < 4:
  print 'Too few arguments, please specify arguments as under:\n  1st argument: Scalastyle XML report file name \n  2nd', \
      'argument: XSL file name\n  3rd argument: Scalastyle HTML report name to be generated...'
  sys.exit(1)

print 'Generating Scalastyle HTML report'

# Check if input Scalastyle XML report exists
xmlReportFileName = sys.argv[1]
if not path.isfile(xmlReportFileName):
  print 'Scalastyle XML report {0} not found. Cannot generate HTML report!'.format(xmlReportFileName)
  sys.exit(1)

# Check if input XSL which will be used to transform XML report exists 
xslFileName = sys.argv[2]
if not path.isfile(xslFileName):
  print 'XSL file {0} for Scalastyle XML report conversion not found. Cannot generate HTML report!'.format(xslFileName)
  sys.exit(1)

# HTML report name which will be outputted
htmlReportFileName = sys.argv[3]

htmlReportFD = None
try:
  xmlreport_root = ET.parse(xmlReportFileName)
  xslt = ET.parse(xslFileName)
  transform = ET.XSLT(xslt)
  # Pass reporttype to XSL to ensure scalstyle specific changes can be made while outputting HTML report.
  htmlreport_root = transform(xmlreport_root, reporttype="'scalastyle'")
  htmlstring = ET.tostring(htmlreport_root, pretty_print=True)
  htmlReportFD = os.open(htmlReportFileName, os.O_RDWR|os.O_CREAT)
  os.write(htmlReportFD, htmlstring)
except:
  print 'Issue encountered during Scalastyle HTML report generation...{0} occured.'.format(sys.exc_info()[0])
  desired_trace = traceback.format_exc(sys.exc_info())
  print(desired_trace)
  sys.exit(1)
finally:
  if htmlReportFD is not None:
    os.close(htmlReportFD)
