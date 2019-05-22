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

import de.johoop.findbugs4sbt.FindBugs._
import de.johoop.findbugs4sbt.Priority

findbugsSettings

// Only High priority Findbugs issues will be generated
findbugsPriority := Priority.High

// Exclude filters file to filter out certain files, folders or issues
findbugsExcludeFilters := Some(scala.xml.XML.loadFile(baseDirectory.value / "project" / "findbugs-exclude-filters.xml"))
