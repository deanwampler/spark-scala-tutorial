#!/bin/bash

dir=$(dirname $0)
. $dir/scripts/find_cmds

help() {
  cat <<EOF
  usage: $0 [-h | --help] [--mem N] [options]

  where:
    -h | --help        Print help and exit.
    --mem N            The default memory for Activator is 4096 (MB), which is also used
                       to run the non-Hadoop Spark examples. Use a larger integer value
                       N if you experience out of memory errors.
    options            Any additional options to pass to SBT.
EOF
}

java_opts() {
  mem=$1
  perm=$(( $mem / 4 ))
  (( $perm < 512 )) && perm=512
  echo "-Xms${mem}M -Xmx${mem}M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=${perm}M"
}

mem=4096
tool=sbt
while [ $# -gt 0 ]
do
  case $1 in
    -h|--h*)         help; exit 0      ;;
    --mem)           shift; mem=$1     ;;
    *)               break             ;;
  esac
  shift
done

dir=$(dirname $0)
ip=$($dir/scripts/getip.sh)

act=$(find_sbt)

if [[ -z $act ]]
then
  echo "ERROR: Could not find $tool"
  exit 1
fi

log="$dir/$tool.log"
JAVA_OPTS=$(java_opts $mem)

echo "Running SBT. Logging to $log"
echo running: JAVA_OPTS=\"$JAVA_OPTS\" "$act" "${opts[@]}"
[[ -z $NOOP ]] && ( JAVA_OPTS="$JAVA_OPTS" "$act" "${opts[@]}" 2>&1 | tee "$log" )
