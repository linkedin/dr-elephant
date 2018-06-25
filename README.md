# Dr. Elephant

[![Build Status](https://api.travis-ci.org/linkedin/dr-elephant.svg)](https://travis-ci.org/linkedin/dr-elephant/)
[![Join the chat at https://gitter.im/linkedin/dr-elephant](https://badges.gitter.im/linkedin/dr-elephant.svg)](https://gitter.im/linkedin/dr-elephant?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<a href=""><img src="images/wiki/dr-elephant-logo-150x150.png" align="left" hspace="10" vspace="6"></a>

**Dr. Elephant** is a performance monitoring and tuning tool for Hadoop and Spark. It automatically gathers all the metrics, runs analysis on them, and presents them in a simple way for easy consumption. Its goal is to improve developer productivity and increase cluster efficiency by making it easier to tune the jobs. It analyzes the Hadoop and Spark jobs using a set of pluggable, configurable, rule-based heuristics that provide insights on how a job performed, and then uses the results to make suggestions about how to tune the job to make it perform more efficiently.

## Documentation

For more information on Dr. Elephant, check the wiki pages [here](https://github.com/linkedin/dr-elephant/wiki).

For quick setup instructions: [Click here](https://github.com/linkedin/dr-elephant/wiki/Quick-Setup-Instructions)

Developer guide: [Click here](https://github.com/linkedin/dr-elephant/wiki/Developer-Guide)

Administrator guide: [Click here](https://github.com/linkedin/dr-elephant/wiki/Administrator-Guide)

User guide: [Click here](https://github.com/linkedin/dr-elephant/wiki/User-Guide)

Engineering Blog: [Click here](https://engineering.linkedin.com/blog/2016/04/dr-elephant-open-source-self-serve-performance-tuning-hadoop-spark)

## Mailing-list & Github Issues

Google groups mailing list: [Click here](https://groups.google.com/forum/#!forum/dr-elephant-users)

Github issues: [click here](https://github.com/linkedin/dr-elephant/issues)

## Meetings

We have scheduled a weekly Dr. Elephant meeting for the interested developers and users to discuss future plans for Dr. Elephant. Please [click here](https://github.com/linkedin/dr-elephant/issues/209) for details.

## How to Contribute?

Check this [link](https://github.com/linkedin/dr-elephant/wiki/How-to-Contribute%3F).

## How to compile and launch on Gaia3

1. Clone the project
	* git clone https://yourAccount@outils-communs-pastel.ts-tlse.fr/gitlab/GAIA/Dr-elephant.git
	
2. Global variables
	* HTTP_PROXY and HTTPS_PROXY
	* Set LANG to en_US.UTF-8
	* Add activator*/bin/ in PATH
	
3. Database
	* Start the service mysql.
	* Default account is drelephant with pwd = "Dr-elephant123"
	* Create your account  or use default account.
	* Create a database or use default database (default datadase is "drelephant")
	
4. Compile
	* In compil.sh you can add or remove tests.
		* Replcace "play_command $OPTS clean compile dist"by "play_command $OPTS clean compile test dist"
	* Run ./compile.sh compile.conf
	
5. Start & Stop
	* After compilation:
		* cd dist/; unzip dr-elephant*.zip; cd dr-elephant*
		* Edit the following parameters in file app-conf/elephant.conf : port, db_url, db_name, db_user and db_password;
	* Launch dr.Elephant -> ./bin/start.sh app-conf/ and go to localhost: "port" to use web UI.
	* To stop the application type ./bin/stop


## License

    Copyright 2016 LinkedIn Corp.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
