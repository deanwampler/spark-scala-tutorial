#!/bin/bash
#========================================================================
# invertedindex5b.sh - Alternative command-line way to invoke InvertedIndex5b on Hadoop.
#========================================================================

output=output/inverted-index
dir=$(dirname $0)
$dir/hadoop.sh --class InvertedIndex5b --output "$output" "$@"


