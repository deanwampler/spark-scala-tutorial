#!/bin/bash
#====================================================================
# sparkshell.sh - Wraps invocation of the spark-shell for interactive use.
#====================================================================

help() {
  cat <<EOF
  usage: $0 [--master master] [--deploy-mode client|cluster] [--jars jars] [other_spark_options]
  where:
    --master master       Defaults to "yarn"
    --deploy-mode         Defaults to cluster
    --jars jars           Comma-separated list. We prepend the "activator-spark*.jar"
    other_spark_options   Any other Spark Shell options
EOF
}

master="yarn"
deploy_mode="cluster"
jars="$HOME/spark-workshop/target/scala-2.10/activator-spark_2.10-2.1.0.jar"
while [ $# -gt 0 ]
do
  case $1 in
    -h|--help)
      help
      exit 0
      ;;
    --master)
      shift
      master=$1
      ;;
    --deploy*)
      shift
      deploy_mode=$1
      ;;
    --jars)
      shift
      jars="$jars,$1"
      ;;
    *)
      break
      ;;
  esac
  shift
done

echo running: $HOME/spark/bin/spark-shell --master $master --deploy-mode $deploy_mode \
  --jars "$jars" $@
echo ""

# use NOOP=x scripts/hadoop.sh ... to suppress execution. You'll just see the
# previous echo output.
if [[ -z $NOOP ]]
then
  $HOME/spark/bin/spark-shell --master $master --deploy-mode $deploy_mode \
    --jars "$jars" $@
fi

echo ""
