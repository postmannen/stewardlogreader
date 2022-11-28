# TODO

- With reply messages, check if it is also registered as locked in the map, if it is not just delete it since it must have been a previous copy that was not finished.
- When the reader is restarted we should check if there already are existing reply messages in the folder for previously restarted copy process, and we should check if they actually finished......if possible.
- Add metrics.
- Add a version flag.
- Files that failed for some reason are never retried, and the reply message is not deleted.
  The problem is most likely that all files in a folder are only scanned at startup and put on the map. After that files are only added to the map if there is a new-file/write event for a file. This means that if a copying a file have been tried, and the reply message for when done was never received, the file will never be retried unless it is written to again, which will not happen with older log files.

## Other
