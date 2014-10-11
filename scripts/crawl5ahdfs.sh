#!/bin/bash
#========================================================================
# crawl5ahdfs.sh - Alternative command-line way to invoke Crawl5ahdfs on Hadoop.
#========================================================================

output=output/crawl
dir=$(dirname $0)
$dir/hadoop.sh --class Crawl5aHDFS --output "$output" "$@"



