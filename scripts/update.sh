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


if [ $# -ne 5 ]
then
    echo "usage: "$0" [application type] [name] [className] [viewName] [jar file]"
    exit 1
fi

appliType=$1
name=$2
className=$3
viewName=$4
jarFile=$5


./stop.sh
echo -n ""

python update.py $appliType $name $className $viewName

echo -n "updating ../lib/"$jarFile" ....\n"
cp $jarFile ../lib

./start.sh ../app-conf/


exit 0;
