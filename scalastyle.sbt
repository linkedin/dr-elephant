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
// scalastyle-sbt-plugin specific configurations go in this file
//

// Do not fail on scalastyle errors as we want to baseline error numbers till
// we fix all errors. We would fail the CI build if any new errors are introduced
// through a PR.
scalastyleFailOnError := false

// Scalastyle config file location.
scalastyleConfig := file("project/scalastyle-config.xml")
