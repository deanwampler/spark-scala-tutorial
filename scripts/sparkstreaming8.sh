#!/bin/bash
#========================================================================
# sparkstreaming8.sh - Invoke SparkStreaming8 on Hadoop or locally using sockets.
# usage: sparkstreaming8.sh [--hadoop|--local]
# default is --hadoop.
#========================================================================

dir=$(dirname $0)
root=$(dirname $dir)
. $dir/find_cmds

case $1 in
  --l*) mode=local   ;;
  *)    model=hadoop ;;
esac

output=output/socket-streaming
dir=$(dirname $0)
echo "Output will be written to: $output"
if [[ $mode == local ]]
then
  export ACT=$(find_activator --silent "$HOME/activator/activator")
  if [[ -n $ACT ]]
    ACT="$ACT shell"
  then
    ACT=$(find_sbt)
    [[ -z $ACT ]] && exit 1
  fi
  if [[ -n $NOOP ]]
  then
    echo "echo run-main SparkStreaming8 --socket localhost:9900 --output $output | $ACT"
  else
    echo run-main SparkStreaming8 --socket localhost:9900 --out "$output" | $ACT
  fi
else
  $dir/hadoop.sh --class SparkStreaming8 --out "$output" --socket localhost:9900 "$@"
fi


