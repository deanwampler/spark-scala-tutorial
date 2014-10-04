#!/bin/bash
#========================================================================
# wordcount3.sh - Alternative command-line way to invoke WordCount3 on Hadoop.
#========================================================================

output=output/kjv-wc3
dir=$(dirname $0)
$dir/hadoop.sh --class WordCount3 --output "$output" "$@"


