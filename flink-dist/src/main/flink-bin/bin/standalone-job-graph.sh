#!/usr/bin/env bash
USAGE="Usage: standalone-job-graph.sh ((start|start-foreground))|stop [args]"

STARTSTOP=$1
ENTRY_POINT_NAME="standalonejobgraph"

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Startup parameters
ARGS=("--configDir" "${FLINK_CONF_DIR}" "${@:2}")

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    # Add cluster entry point specific JVM options
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_JM}"
    parseJmJvmArgsAndExportLogs "${ARGS[@]}"
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh ${ENTRY_POINT_NAME} "${ARGS[@]}"
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh ${STARTSTOP} ${ENTRY_POINT_NAME} "${ARGS[@]}"
fi
