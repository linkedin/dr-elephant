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

//import play.Project._
import play.sbt.PlayImport._
import Dependencies._

name := "dr-elephant"

version := "2.1.7"

organization := "com.linkedin.drelephant"

// Enable CPD SBT plugin
lazy val root = (project in file(".")).enablePlugins(CopyPasteDetector)

javacOptions in Compile ++= Seq("-source", "1.8", "-target", "1.8")

playEbeanModels in Compile := Seq("models.*")
playEbeanDebugLevel := 9

libraryDependencies ++= dependencies map { _.excludeAll(exclusionRules: _*) }

// Create a new custom configuration called compileonly
ivyConfigurations += config("compileonly").hide

// Append all dependencies with 'compileonly' configuration to unmanagedClasspath in Compile.
unmanagedClasspath in Compile ++= update.value.select(configurationFilter("compileonly"))

//playJavaSettings
lazy val myProject = (project in file(".")).enablePlugins(PlayJava, PlayEbean)

scalaVersion := "2.11.11"

envVars in Test := Map("PSO_DIR_PATH" -> (baseDirectory.value / "scripts/pso").getAbsolutePath)
