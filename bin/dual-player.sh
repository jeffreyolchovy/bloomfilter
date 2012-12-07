#!/bin/bash

BIN_DIR='/Users/jeffo/Projects/jvm/bloomfilter/bin'

main() {
  local file1=$1
  local file2=$2

  tmux split-window -v

  sleep 1

  tmux send-keys -t 0 "clear" C-m
  tmux select-pane -t 0

  tmux select-pane -t 0
  tmux send-keys -t 0 "$BIN_DIR/player.sh $file1" C-m
  tmux select-pane -t 1
  tmux send-keys -t 1 "$BIN_DIR/player.sh $file2" C-m
}

main $1 $2
