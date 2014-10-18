#!/bin/bash
#====================================================================
# hadoop.sh - Wraps calls to spark-submit for submitting Spark jobs.
#====================================================================

dir=$(dirname $0)
root=$(dirname $dir)
. $dir/find_cmds

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

export HADOOP=$(find_hadoop)
[[ -z $HADOOP ]] && exit 1
# echo "Using hadoop command: $HADOOP"

export SPARK_SUBMIT=$(find_spark_submit)
[[ -z $SPARK_SUBMIT ]] && exit 1
# echo "Using spark submit command: $SPARK_SUBMIT"

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

# TODO: This only works if $output is really the output directory, not a prefix.
# See below, where once the output directories exist, we correctly find them.
$HADOOP fs -rm -r -f $output

project_jar=$(find $root/target/scala-2.* -name 'activator-spark_*.jar' | grep -v 'tests.jar')

echo running: $SPARK_SUBMIT --master $master --class $main \
  $project_jar --master $master --out $output $@
echo ""

# use NOOP=x scripts/hadoop.sh ... to suppress execution. You'll just see the
# previous echo output.
if [[ -z $NOOP ]]
then
  $SPARK_SUBMIT --master $master --class $main \
    $project_jar --master $master --out $output $@
fi

echo ""
outputs=($output)
$HADOOP fs -test -d $output
if [ $? -eq 0 ]
then
  echo "Contents of output directory:"
else
  echo "Contents of the output directories:"
  output2=$(dirname $output)
  outputs=($($HADOOP fs -ls $output2 | grep $output | sed -e "s?.*\($output.*\)?\1?"))
fi

for o in ${outputs[@]}
do
  echo " **** $o:"
  $HADOOP fs -ls $o
  echo ""
done

ip=$($dir/getip.sh)
echo ""
echo " **** To see the contents, open the following URL(s):"
echo ""
for o in ${outputs[@]}
do
  echo "      http://$ip:8000/filebrowser/view/$o"
done
echo ""
