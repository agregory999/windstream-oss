# Windstream-oss - Python SDK + OSS
Python scripts to fully utilize python SDK and OSS multi-part

## Algorithm (proposed)_

### Constants
large file = >1GB
concurrency = 10

### Recursive Process
Start at top level:

identify large files - for each large file, kick off subprocess for multi-part

open local tar file for writing
each local file is added to tar
large files are excluded

directory found - recurse

Once done with listing, close tar file and call large_file routine
delete local tar file


### Subroutines
large_file - use multi-part with UploadManager, use file permissions as metadata

