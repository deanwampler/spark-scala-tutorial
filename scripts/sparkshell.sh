#!/bin/bash
#====================================================================
# sparkshell.sh - Wraps invocation of the spark-shell for interactive use.
#====================================================================

dir=$(dirname $0)
root=$(dirname $dir)
. $dir/find_cmds

help() {
  cat <<EOF
  usage: $0 [--master master] [--jars jars] [other_spark_options]
  where:
    --master master       Defaults to "local[*]"
    --jars jars           Comma-separated list. We prepend the project jar already.
    other_spark_options   Any other Spark Shell options
EOF
}

help2() {
  cat <<EOF

  Output of $SPARK_SHELL --help:

EOF
  $SPARK_SHELL --help
}

export SPARK_SHELL=$(find_spark_shell)
[[ -z $SPARK_SHELL ]] && exit 1
# echo "Using spark shell command: $SPARK_SHELL"

project_jar=$(find $root/target/scala-2.* -name 'spark-scala-tutorial_*.jar' | grep -v 'tests.jar')
jars="$project_jar"
args=()
while [ $# -gt 0 ]
do
  case $1 in
    -h|--help)
      help
      help2
      exit 0
      ;;
    --jars)
      shift
      jars="$jars,$1"
      ;;
    *)
      args[${#args[@]}]=$1
      ;;
  esac
  shift
done

echo running: $SPARK_SHELL --jars "$jars" ${args[@]}
echo ""

# use NOOP=x scripts/hadoop.sh ... to suppress execution. You'll just see the
# previous echo output.
if [[ -z $NOOP ]]
then
  $SPARK_SHELL --jars "$jars" ${args[@]}
fi

echo ""
