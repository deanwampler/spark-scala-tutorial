#!/bin/bash
getip() {
  ip addr show scope global | grep inet | sed -e 's/\s*inet //' | cut -d/ -f 1
}

echo "===================================================="
echo ""
echo "    Starting the Spark Workshop in Activator..."
echo "    Open your web browser to $(getip):9999"
echo ""
echo "===================================================="
sleep 2
$HOME/activator/activator -Dhttp.address=0.0.0.0 -Dhttp.port=9999 ui
