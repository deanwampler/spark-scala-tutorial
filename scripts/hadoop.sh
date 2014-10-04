#!/bin/bash
#====================================================================
# hadoop.sh - Wraps calls to spark-submit for submitting Spark jobs.
#====================================================================

help() {
  cat <<EOF
  usage: $0 --class main -o | --out output_path [--master master] [app_options]
  where:
    --class main       Specifies the "main" routine to run in the app jar.
    --out output_path  Specifies the output location.
                       Although all the apps have defaults for this path,
                       this script needs to know it!
    --master master    Defaults to "yarn-client"
    app_options        Any other options to pass to the app.
EOF
}

getip() {
  ip addr show scope global | grep inet | sed -e 's/\s*inet //' | cut -d/ -f 1
}

master="yarn-client"
main=""
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
    --class)
      shift
      main=$1
      ;;
    -o|--out*)
      shift
      output=$1
      ;;
    *)
      break
      ;;
  esac
  shift
done

if [[ -z $main ]]
then
  echo "$0: Must specify a --class main argument"
  help
  exit 1
fi
if [[ -z $output ]]
then
  echo "$0: Must specify a --output output_path argument"
  help
  exit 1
fi

dir=$(dirname $0)

hadoop fs -rm -r -f $output

echo running: $HOME/spark/bin/spark-submit --master $master --class $main \
  $HOME/spark-workshop/target/scala-2.10/activator-spark_2.10-2.1.0.jar \
  --out $output $@
echo ""

# use NOOP=x scripts/hadoop.sh ... to suppress execution. You'll just see the
# previous echo output.
if [[ -z $NOOP ]]
then
  $HOME/spark/bin/spark-submit --master $master --class $main \
    $HOME/spark-workshop/target/scala-2.10/activator-spark_2.10-2.1.0.jar \
    --out $output $@
fi

echo ""
echo "Contents of $output:"
hadoop fs -ls $output

echo ""
echo " **** To see the output, open the following URL:"
echo "      http://$(getip):8000/filebrowser/view/$output"
