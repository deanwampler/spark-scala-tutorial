#!/bin/bash
#========================================================================
# sparksql9.sh - Alternative command-line way to invoke SparkSQL9 on Hadoop.
#========================================================================

output=output/spark-sql
dir=$(dirname $0)
$dir/hadoop.sh --class SparkSQL9 --output "$output" "$@"



