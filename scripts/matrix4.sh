#!/bin/bash
#========================================================================
# matrix4.sh - Alternative command-line way to invoke Matrix4 on Hadoop.
#========================================================================

output=output/matrix-math
dir=$(dirname $0)
$dir/hadoop.sh --class Matrix4 --output "$output" "$@"


