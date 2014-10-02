#!/bin/bash
getip() {
  ip addr show scope global | grep inet | sed -e 's/\s*inet //' | cut -d/ -f 1
}

echo "==========================================="
echo "Starting the Spark Workshop in Activator..."
echo "Open your web browser to $(getip):8888"
echo "==========================================="

echo $HOME/activator/activator ui &
