#!/bin/bash

dir=$(dirname $0)
. $dir/scripts/find_cmds

help() {
  cat <<EOF
  usage: $0 [-h | --help] [act | activator | sbt ] [ui | shell] \
            [--mem N] [-nq | --not-quiet]  [options]

  where:
    -h | --help        Print help and exit.
    act | activator    Use Activator (the default).
    sbt                Use SBT (command line interface).
    ui | shell         (Activator only) Use the Web UI (default) or the command line shell.
    --mem N            The default memory for Activator is 4096 (MB), which is also used
                       to run the non-Hadoop Spark examples. Use a larger integer value
                       N if you experience out of memory errors.
    -nq|--not-quiet
                       By default, most Activator output is suppressed when the web UI is
                       used. For debugging purposes when running the web UI, use this
                       option to show this output.
    options            Any additional options to pass to Activator or SBT.

  The default is Activator and the web-based UI.
EOF
}

filter_garbage() {
  quiet=$1
  while read line
  do
    if [[ ${line} =~ Please.point.your.browser.at ]] ; then
      echo
      echo "=================================================================="
      echo
      echo "    Open your web browser to:   http://$ip:9999"
      echo
      echo "=================================================================="
    elif [[ ${line} =~ play.-.Application.started ]] ; then
      echo $line
    elif [[ -z $quiet ]] ; then
      echo $line
    fi
  done
}

java_opts() {
  mem=$1
  perm=$(( $mem / 4 ))
  (( $perm < 512 )) && perm=512
  echo "-Xms${mem}M -Xmx${mem}M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=${perm}M"
}

mem=4096
quiet=yes
mode=ui
tool=activator
while [ $# -gt 0 ]
do
  case $1 in
    -h|--h*)         help; exit 0      ;;
    --mem)           shift; mem=$1     ;;
    -nq|--not-quiet) quiet=     ;;
    act*)            tool=activator    ;;
    sbt*)            tool=sbt; mode=   ;;
    ui|shell)        mode=$1           ;;
    *)               break             ;;
  esac
  shift
done

if [[ $tool = sbt && $mode = ui ]]
then
  echo "$0: Can't specify 'sbt' and 'ui' together."
  help
  exit 1
fi

dir=$(dirname $0)
ip=$($dir/scripts/getip.sh)

act=
opts=()
msg=
if [[ $tool = activator ]]
then
  act=$(find_activator "$HOME/activator/activator")
  # Use http.address=0.0.0.0, because when running on a remote server,
  # The Activator UI won't listen for remote connections otherwise.
  # Invoke with NOOP=x start.sh to suppress execution:
  opts=(-Dhttp.address=0.0.0.0 -Dhttp.port=9999 $mode)
  umode=$(echo $mode | tr '[a-z]' '[A-Z]')
  msg="Activator, using mode $umode"
else
  act=$(find_sbt)
  msg="SBT"
fi

if [[ -z $act ]]
then
  echo "ERROR: Could not find $tool"
  exit 1
fi

if [[ $mode = ui ]]
then
  cat <<EOF
==================================================================

    Starting the Spark Workshop in $msg

==================================================================
EOF
  sleep 2
fi

log="$dir/$tool.log"
JAVA_OPTS=$(java_opts $mem)

if [[ $mode = ui ]]
then
  echo "Running the Web UI. Writing all activity to $log"
  echo running: JAVA_OPTS=\"$JAVA_OPTS\" "$act" "${opts[@]}"
  [[ -z $NOOP ]] && ( JAVA_OPTS="$JAVA_OPTS" "$act" "${opts[@]}" 2>&1 | tee "$log" | filter_garbage $quiet )
else
  echo "Running the command shell. Writing all activity to $log"
  echo running: JAVA_OPTS=\"$JAVA_OPTS\" "$act" "${opts[@]}"
  [[ -z $NOOP ]] && ( JAVA_OPTS="$JAVA_OPTS" "$act" "${opts[@]}" 2>&1 | tee "$log" )
fi
