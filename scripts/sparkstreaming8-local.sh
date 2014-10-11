#!/bin/bash
#========================================================================
# sparkstreaming8-local.sh - Invoke SparkStreaming8 in local mode using sockets.
#========================================================================

output=output/socket-streaming
echo "Output will be written to: $output"

echo run-main SparkStreaming8 --socket localhost:9900 --output "$output" | $HOME/activator/activator shell

