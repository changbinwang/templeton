#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The file containing the running pid
PID_FILE=./templeton.pid

# The console error log
ERROR_LOG=./templeton-console-error.log

# The console log
CONSOLE_LOG=./templeton-console.log

# The name of the templeton jar file
TEMPLETON_JAR=templeton-0.1.0-dev.jar

# How long to wait before testing that the process started correctly
SLEEP_TIME_AFTER_START=10



