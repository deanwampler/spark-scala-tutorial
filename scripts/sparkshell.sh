#!/bin/bash
#====================================================================
# sparkshell.sh - Wraps invocation of the spark-shell for interactive use.
#====================================================================

help() {
  cat <<EOF
  usage: $0 [--master master] [--jars jars] [other_spark_options]
  where:
    --master master       Defaults to "local[*]"
    --jars jars           Comma-separated list. We prepend the "activator-spark*.jar"
    other_spark_options   Any other Spark Shell options

  Output of $HOME/spark/bin/spark-shell --help:

EOF
  $HOME/spark/bin/spark-shell --help
}

project_jar=$(find $HOME/spark-workshop/target/scala-2.* -name 'activator-spark_*.jar' | grep -v 'tests.jar')
jars="$project_jar"
args=()
while [ $# -gt 0 ]
do
  case $1 in
    -h|--help)
      help
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

echo running: $HOME/spark/bin/spark-shell --jars "$jars" ${args[@]}
echo ""

# use NOOP=x scripts/hadoop.sh ... to suppress execution. You'll just see the
# previous echo output.
if [[ -z $NOOP ]]
then
  $HOME/spark/bin/spark-shell --jars "$jars" ${args[@]}
fi

echo ""
