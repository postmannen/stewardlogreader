# TODO

- With reply messages, check if it is also registered as locked in the map, if it is not just delete it since it must have been a previous copy that was not finished.
- When the reader is restarted we should check if there already are existing reply messages in the folder for previously restarted copy process, and we should check if they actually finished......if possible.
- Add metrics.
- Add a version flag.
- If the copy have failed for some reason we are left with initial reply file, but it seems like the copy process of that same file is never started again.

## Other

1.1 is branch with copy new values to map.
1.1.1 is same as 1.1 but testing with pointers for map values, but not merged into 1.1

1.1.2 is same as 1.1, but further testing with still copy values with map
