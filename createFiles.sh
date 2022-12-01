#!/bin/bash

for ((i = 1; i <= 10; ++i)); do
    touch ./logs/"$i".txt
    echo "this file contains: $i" >./logs/"$i".txt
done
