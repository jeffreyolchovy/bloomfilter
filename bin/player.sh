#!/bin/bash

main() {
  local file=$1
  local pause=$2

  clear
  while read line
  do
    if [ "$line" == "*" ]; then
      sleep $pause
      clear
    else
      echo -e $line
    fi
  done < $file
  clear
}

FILE=$1
PAUSE=${2:-3}

main $FILE $PAUSE
