#!/bin/bash
#========================================================================
# ngrams6.sh - Alternative command-line way to invoke NGrams6 on Hadoop.
#========================================================================

output=output/ngrams
dir=$(dirname $0)
$dir/hadoop.sh --class NGrams6 --output "$output" "$@"


