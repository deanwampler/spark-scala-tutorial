#!/bin/bash
#========================================================================
# sparkstreaming11.sh - Invoke SparkStreaming11 on Hadoop.
# Supports the socket input configuration only, using the DataSocketServer.
# TODO: Some of the hard-coded values could be script options.
#========================================================================

dir=$(dirname $0)
root=$(dirname $dir)
. $dir/find_cmds

datafile=data/kjvdat.txt
output=output/socket-streaming
dir=$(dirname $0)
echo "Output will be written to: $output"

export ACT=$(find_activator --silent "$HOME/activator/activator")
if [[ -n $ACT ]]
  ACT="$ACT shell"
then
  ACT=$(find_sbt)
  [[ -z $ACT ]] && exit 1
fi

echo "Starting the DataSocketServer:"
echo "  echo run-main com.typesafe.sparkworkshop.util.streaming.DataSocketServer 9900 $datafile | $ACT"
if [[ -z $NOOP ]]
  echo run-main com.typesafe.sparkworkshop.util.streaming.DataSocketServer 9900 $datafile | $ACT &
fi
sleep 1
echo "Starting SparkStreaming11"
  $dir/hadoop.sh --class SparkStreaming11 --out "$output" --socket localhost:9900 "$@"
fi


