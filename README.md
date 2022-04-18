# Windstream-oss - Python SDK + OSS
Python scripts to fully utilize python SDK and OSS multi-part

## Algorithm 

uploadOSS.py
- Given a local folder, walk the tree and upload all files to OSS
- Use parallel multipart uploads if the file exceeds 128M
- Use OS Process Pool to define external processes to do the work.

clean_bucket.py
- Generates a listing of an bjoect bucket and deletes every object in it

