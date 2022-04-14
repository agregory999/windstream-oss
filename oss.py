import time
import os
import oci
import os
from oci.object_storage import UploadManager
from oci.object_storage import object_storage_client
from oci.object_storage.transfer.constants import DEFAULT_PART_SIZE
from array import array
from pathlib import Path

from multiprocessing import Semaphore
from multiprocessing import Process

# Concurrency
concurrent = 10

# The root directory path, Replace with your path
p = Path('/home/andrew_gre/windstream-python')
#p = Path('/home/andrew_gre/windstream-python/files')

# The Compartment OCID
compartment_id = "ocid1.compartment.oc1..aaaaaaaasczurylkzvviqfujhtinyiycg27yelt4opnwo3rgsfu22cuehv6q"

# The Bucket name where we will upload
#bucket_name = "bucket-directory-upload"
bucket_name = "directory-upload"

def progress_callback(bytes_uploaded):
    return
    #print("{} additional bytes uploaded".format(bytes_uploaded))

def actualUpload(path:str,name:str,object_storage_client,upload_manager,namespace):
  with open(path, "rb") as in_file:
    print(f"File Size: {os.stat(path).st_size}")
    if os.stat(path).st_size > 100000000:
        # Use multi-part
        print("Starting multi-part upload {}", format(name))

        div = 8
        #upload_manager = UploadManager(object_storage_client, allow_parallel_uploads=True, parallel_process_count=8)
        start = time.time()
        response = upload_manager.upload_file(
            namespace, 
            bucket_name, 
            name, 
            path, 
            part_size=int(DEFAULT_PART_SIZE / div) , 
            progress_callback=progress_callback)
        end = time.time()

        print(f"Finished uploading: {name} Time: {end - start}s")
    else:
        print(f"Starting upload {name}")
        object_storage_client.put_object(namespace,bucket_name,name,in_file)
        print("Finished uploading {}".format(name))
  sema.release()    

def uploadOSS(path:str,name:str,object_storage_client,upload_manager,namespace,proc_list):
#  print(f"Acquiring semaphore - proc list: {proc_list}")
  sema.acquire()
  process = Process(target=actualUpload, args=(path,name,object_storage_client,upload_manager,namespace))
  proc_list.append(process)
  process.start()



def processDirectoryLocal(path:Path,object_storage_client,upload_manager,namespace,proc_list):
  print(f"Processing Directory {path}")
  if path.exists():
    print("in directory ---- " + path.relative_to(p).as_posix())
    for object in path.iterdir():
      if object.is_dir():
        print(f"Recurse Directory {object}")
        processDirectoryLocal(object,object_storage_client,upload_manager,namespace,proc_list)
      else:
        print(f"Process File {object.as_posix()}")
        
        uploadOSS(object.as_posix(),object.relative_to(p).as_posix(),object_storage_client,upload_manager,namespace,proc_list)



if __name__ == '__main__':

  config = oci.config.from_file()
  object_storage_client = oci.object_storage.ObjectStorageClient(config)
  upload_manager = UploadManager(object_storage_client, allow_parallel_uploads=True, parallel_process_count=8)
  namespace = object_storage_client.get_namespace().data

  proc_list: array = []
  sema = Semaphore(concurrent)

if p.exists() and p.is_dir():
  processDirectoryLocal(p,object_storage_client,upload_manager,namespace,proc_list)

for job in proc_list:
  job.join()
