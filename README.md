# stewardlogreader

Reads archived logs, creates a message with the log as it's payload, and ships it off to some destination

## Flags

```bash
  -checkInterval int
        the check interval in seconds (default 5)
  -copyChunkSize string
        the chunk size to split files into while copying
  -copyDstFolder string
        the folder at the destination to write files to.
  -copyDstToNode string
        the node to send the messages created to
  -copyMaxTransferTime string
        the max time a copy transfer operation are allowed to take in seconds
  -copySrcFolder string
        the folder to watch
  -maxFileAge int
        how old a single file is allowed to be in seconds before it gets read and sent to the steward socket (default 60)
  -msgRepliesFolder string
        the folder where steward will deliver reply messages for when the dst node have received the copy request
  -msgToNode string
        the name of the (this) local steward instance where we inject messages on the socket
  -prefixName string
        name to be prefixed to the file name
  -prefixTimeNow
        set to true to prefix the filename with the time the file was piced up for copying
  -socketFullPath string
        the full path to the steward socket file
```
