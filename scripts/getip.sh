#!/bin/bash
# get the IP address.

# Chet for MacOS, just for testing purposes
if [ $(uname) = "Darwin" ]
then
  echo localhost
else
  ip addr show scope global | grep inet | sed -e 's/\s*inet //' | cut -d/ -f 1
fi
