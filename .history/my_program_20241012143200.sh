#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how your program is running.
#

set -e # Exit early if any commands fail

#
# - Edit this to change how your program compiles locally

(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  go build -o /tmp/codecrafters-build-kafka-go app/*.go
)


#
# - Edit this to change how your program runs locally
exec /tmp/codecrafters-build-kafka-go "$@"
