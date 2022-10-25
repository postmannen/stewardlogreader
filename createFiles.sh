#!/bin/bash

for (( i = 1 ; i <= 20 ; ++i )) ; do
    touch ./logs/$i.txt
done
