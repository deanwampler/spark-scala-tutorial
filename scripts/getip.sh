#!/bin/bash
# get the IP address.

ip addr show scope global | grep inet | sed -e 's/\s*inet //' | cut -d/ -f 1
