#!/bin/bash

for ((i = 1; i <= 100; ++i)); do
    dd if=/dev/urandom of=./logs.nosync/"$i".txt bs=1K count=1
    sleep 0.1
    # echo "this file contains: $i" >./logs/"$i".txt
done
