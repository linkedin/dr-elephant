//
// Copyright 2016 LinkedIn Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.
//

//
// sbt-checkstyle-plugin specific configurations go in this file
//

// Path and name of checkstyle configuration file
checkstyleConfigLocation := CheckstyleConfigLocation.File("project/checkstyle-config.xml")

// Generate HTML report in addition to default XML report by applying XSLT transformations
checkstyleXsltTransformations := {
  Some(Set(CheckstyleXSLTSettings(baseDirectory(_ / "project/checkstyle-noframes-severity-sorted-modified.xsl").value, target(_ / "checkstyle-report.html").value)))
}
