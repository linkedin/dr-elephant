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
// cpd4sbt plugin settings for integrating with CPD which is used for code duplication
//
import de.johoop.cpd4sbt._

// By default language will be Java but this will be changed to run for Scala as well
// while running build through Travis CI.
cpdLanguage := Language.Java

// Take distinct source directories to ensure whole file is not reported as duplicate
// of itself.
cpdSourceDirectories in Compile := (cpdSourceDirectories in Compile).value.distinct
