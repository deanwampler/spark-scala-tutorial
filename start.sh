#!/bin/bash

help() {
  echo "usage: $0 [-h|--help] [ui|shell|sbt]"
  echo "where: ui is the default and sbt is an alias for shell."
  echo "Use ui for the web-based UI. Use shell/sbt for the command line."
}

case $1 in
  ui|shell) mode=$1    ;;
  sbt)      mode=shell; echo "Using shell mode" ;;
  "")       mode=ui    ;;
  -h|--h*)
    help
    exit 0
    ;;
  *)
    echo "Unrecognized argument $1"
    help
    exit 1
    ;;
esac

dir=$(dirname $0)
ip=$($dir/scripts/getip.sh)

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
  echo "    (Ignore the message that will say 'http://0.0.0.0:9999')"
  echo
  echo "=================================================================="

  sleep 2
fi

# Invoke with NOOP=x start.sh to suppress execution:
echo $HOME/activator/activator -Dhttp.address=0.0.0.0 -Dhttp.port=9999 $mode
[[ -z $NOOP ]] && $HOME/activator/activator -Dhttp.address=0.0.0.0 -Dhttp.port=9999 $mode
