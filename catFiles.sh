#!/bin/bash

for ((i = 1; i <= 10; ++i)); do
    rm -rf tmp/clear"$i".iso
    cat clear-36010-live-server.iso >tmp/clear"$i".iso &
done
