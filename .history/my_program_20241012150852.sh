#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how your program runs.

set -e # Exit early if any commands fail

#
# - Edit this to change how your program compiles locally
(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  go build -o /tmp/build-kafka-go app/*.go
)

#
# - Edit this to change how your program runs locally
exec /tmp/build-kafka-go "$@"
