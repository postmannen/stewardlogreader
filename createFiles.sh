#!/bin/bash

for ((i = 1; i <= 1; ++i)); do
    dd if=/dev/urandom of=./logs.nosync/"$i".txt bs=64K count=1
    # echo "this file contains: $i" >./logs/"$i".txt
done
