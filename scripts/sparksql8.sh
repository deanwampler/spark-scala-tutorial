#!/bin/bash
#========================================================================
# sparksql8.sh - Alternative command-line way to invoke SparkSQL8 on Hadoop.
#========================================================================

output=output/spark-sql
dir=$(dirname $0)
$dir/hadoop.sh --class SparkSQL8 --output "$output" "$@"



