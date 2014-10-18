#!/bin/bash

dir=$(dirname $0)
. $dir/scripts/find_cmds

help() {
  cat <<EOF
  usage: $0 [-h | --help] [act | activator | sbt ] [ui | shell] [options]
  where:
    -h | --help       Print help and exit.
    act | activator   Use Activator (the default).
    sbt               Use SBT (command line interface).
    ui | shell        (Activator only) Use the Web UI (default) or the command line shell.
    options           Any additional options to pass to Activator or SBT.

  The default is Activator and the web-based UI.
EOF
}

tool=activator
mode=ui
while [ $# -gt 0 ]
do
  case $1 in
    -h|--h*)
      help
      exit 0
      ;;
    act*)     tool=activator    ;;
    sbt*)     tool=sbt; mode=   ;;
    ui|shell) mode=$1           ;;
    *)        break             ;;
  esac
  shift
done

if [[ $tool = sbt ]]
then
  if [[ $mode = ui ]]
  then
    echo "$0: Can't specify 'sbt' and 'ui' together."
    help
    exit 1
  elif [[ $mode = shell ]]
  then
    echo "$0: 'shell' argument ignored for sbt"
    mode=
  fi
fi

dir=$(dirname $0)
ip=$($dir/scripts/getip.sh)

export act=
if [[ $tool = activator ]]
then
  act=$(find_activator "$HOME/activator/activator")
  [[ -z $act ]] && exit 1
  # Use http.address=0.0.0.0, because when running on a remote server,
  # The Activator UI won't listen for remote connections otherwise.
  # Invoke with NOOP=x start.sh to suppress execution:
  opts="-Dhttp.address=0.0.0.0 -Dhttp.port=9999"
else
  act=$(find_sbt)
  opts=
  [[ -z $act ]] && exit 1
fi

if [[ $mode = ui ]]
then
  umode=$(echo $mode | tr '[a-z]' '[A-Z]')

  echo "=================================================================="
  echo
  echo "    Starting the Spark Workshop in Activator using $umode mode..."
  echo "    Open your web browser to:"
  echo
  echo "        http://$ip:9999"
  echo
  echo "    (Ignore a message that says 'open http://0.0.0.0:9999')"
  echo
  echo "=================================================================="

  sleep 2
fi

echo running: $act $opts $mode $@
[[ -z $NOOP ]] && $act $opts $mode "$@"
