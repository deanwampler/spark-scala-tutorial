#!/bin/bash

help() {
  echo "usage: $0 [-h|--help] [ui|shell|sbt]"
  echo "where: ui is the default and sbt is an alias for shell."
  echo "Use ui for the web-based UI. Use shell/sbt for the command line."
}

case $1 in
  ui|shell) mode=$1    ;;
  sbt)      mode=shell ;;
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

ip=$(scripts/get_ip.sh)

echo "================================================================"
echo ""
echo "    Starting the Spark Workshop in Activator using $mode mode..."
echo "    Open your web browser to $ip:9999"
echo ""
echo "================================================================"

sleep 2
$HOME/activator/activator -Dhttp.address=0.0.0.0 -Dhttp.port=9999 $mode
