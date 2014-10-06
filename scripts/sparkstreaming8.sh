#!/bin/bash
#========================================================================
# sparkstreaming8.sh - Invoke SparkStreaming8 on Hadoop using sockets.
#========================================================================

output=output/socket-streaming
dir=$(dirname $0)
$dir/hadoop.sh --class SparkStreaming8 --output "$output" --socket localhost:9900 "$@"


