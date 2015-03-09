#!/bin/bash
#========================================================================
# crawl5a.sh - Alternative command-line way to invoke Crawl5a on Hadoop.
#========================================================================

output=output/crawl
dir=$(dirname $0)
$dir/hadoop.sh --class Crawl5a --output "$output" "$@"



