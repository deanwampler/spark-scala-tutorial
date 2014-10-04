#!/bin/bash
#========================================================================
# joins7.sh - Alternative command-line way to invoke Joins7 on Hadoop.
#========================================================================

output=output/kjv-joins
dir=$(dirname $0)
$dir/hadoop.sh --class Joins7 --output "$output" "$@"



