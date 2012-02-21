#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Support functions
#

# Follow symlinks on Linux and Darwin
function real_script_name() {
        local base=$1
        local real
        if readlink -f $base >/dev/null 2>&1; then
                # Darwin/Mac OS X
                real=`readlink -f $base`
        fi
        if [[ "$?" != "0" || -z "$real" ]]; then
                # Linux
                local bin=$(cd -P -- "$(dirname -- "$base")">/dev/null && pwd -P)
                local script="$(basename -- "$base")"
                real="$bin/$script"
        fi
        echo "$real"
}

function usage() {
        echo "usage: $0 [start|stop|foreground]"
        echo "  start           Start the Templeton Server"
        echo "  stop            Stop the Templeton Server"
        echo "  foreground      Run the Templeton Server in the foreground"
        exit 1
}

# Print an error message and exit
function die() {
        echo "templeton: $@" 1>&2
        exit 1
}

# Print an message
function log() {
        echo "templeton: $@"
}

# Find the templeton jar
function find_jar_path() {
        for dir in "." "lib" "build/templeton" "share/templeton/"; do
                local jar="$this_bin/../$dir/$TEMPLETON_JAR"
                if [[ -f $jar ]]; then
                        echo $jar
                        break
                fi
        done
}

# Find the templeton classpath
function find_classpath() {
        local classpath=""
        for dir in "lib" "build/ivy/lib/templeton" "conf" $TEMPLETON_CONF_DIR; do
                local path="$this_bin/../$dir"
                if [[ -d $path ]]; then
                        for jar_or_conf in $path/*; do
                                if [[ -z "$classpath" ]]; then
                                        classpath="$jar_or_conf"
                                else
                                        classpath="$classpath:$jar_or_conf"
                                fi
                        done
                fi
        done
        echo $classpath
}

# Check if the pid is running
function check_pid() {
        local pid=$1
        if ps -p $pid > /dev/null; then
                return 0
        else
                return 1
        fi
}

# Start the templeton server in the foreground
function foreground_templeton() {
        $start_cmd
}

# Start the templeton server in the background.  Record the PID for
# later use.
function start_templeton() {
        if [[ -f $PID_FILE ]]; then
                # Check if there is a server running
                local pid=`cat $PID_FILE`
                if check_pid $pid; then
                        die "already running on process $pid"
                fi
        fi

        log "starting ..."
        nohup $start_cmd >>$CONSOLE_LOG 2>>$ERROR_LOG &
        local pid=$!

        if [[ -z "${pid}" ]] ; then # we failed right off
                die "failed to start"
        fi

        sleep $SLEEP_TIME_AFTER_START

        if check_pid $pid; then
                echo $pid > $PID_FILE
                log "starting ... started."
        else
                die "failed to start"
        fi
}

# Stop a running server
function stop_templeton() {
        local pid
        if [[ -f $PID_FILE ]]; then
                # Check if there is a server running
                local check=`cat $PID_FILE`
                if check_pid $check; then
                        pid=$check
                fi
        fi

        if [[ -z "$pid" ]]; then
                log "no running server found"
        else
                log "stopping ..."
                kill $pid
                sleep $SLEEP_TIME_AFTER_START
                if check_pid $pid; then
                        die "failed to stop"
                else
                        log "stopping ... stopped"
                fi
        fi
}

#
# Build command line and run
#

this=`real_script_name "${BASH_SOURCE-$0}"`
this_bin=`dirname $this`

if [[ -f "$this_bin/../libexec/templeton_config.sh" ]]; then
        . "$this_bin/../libexec/templeton_config.sh"
else
        . "$this_bin/templeton_config.sh"
fi

JAR=`find_jar_path`
if [[ -z "$JAR" ]]; then
        die "No templeton jar found"
fi

CLASSPATH=`find_classpath`
if [[ -z "$CLASSPATH" ]]; then
        die "No classpath or jars found"
fi
CLASSPATH="$JAR:$CLASSPATH"

if [[ -z "$HADOOP_CLASSPATH" ]]; then
        export HADOOP_CLASSPATH="$CLASSPATH"
else
        export HADOOP_CLASSPATH="$CLASSPATH:$HADOOP_CLASSPATH"
fi

export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_OPTS="-Dlog4j.configuration=templeton-log4j.properties"

start_cmd="$HADOOP_PREFIX/bin/hadoop jar $JAR org.apache.hcatalog.templeton.Main"

cmd=$1
case $cmd in
        start)
                start_templeton
                ;;
        stop)
                stop_templeton
                ;;
        foreground)
                foreground_templeton
                ;;
        *)
                usage
                ;;
esac

log "done"
