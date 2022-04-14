import time
import os
import sys
import oci
import os
from oci.object_storage import UploadManager
from oci.object_storage import object_storage_client
from oci.object_storage.transfer.constants import DEFAULT_PART_SIZE
from array import array
from pathlib import Path

from multiprocessing import Semaphore
from multiprocessing import Process

# Concurrency (Default)
concurrency = 5

# The root directory path, Replace with your path
# p = Path('/home/andrew_gre/windstream-python')
#p = Path('/home/andrew_gre/windstream-python/files')

# The Compartment OCID
compartment_id = "ocid1.compartment.oc1..aaaaaaaasczurylkzvviqfujhtinyiycg27yelt4opnwo3rgsfu22cuehv6q"

# The Bucket name where we will upload
#bucket_name = "bucket-directory-upload"
bucket_name = "directory-upload"

# Whether to recurse with subprocess
recurse_subdirectory_process = False

# Multi-part Parallelism
multipart_parallism = 5

def progress_callback(bytes_uploaded):
    return
    #print("{} additional bytes uploaded".format(bytes_uploaded))

def multipartUpload(path: str, name: str, upload_manager, namespace):
    with open(path, "rb") as in_file:
        start = time.time()
        # Actual uplaod as multi-part
        print(f"Started MP uploading: {name}")
        response = upload_manager.upload_file(
                namespace,
                bucket_name,
                name,
                path,
                part_size=int(DEFAULT_PART_SIZE),
                progress_callback=progress_callback)

        end = time.time()
        print(f"Finished MP uploading: {name} Time: {end - start}s")
        sema.release()

def processDirectory(path: Path, object_storage_client, upload_manager, namespace, proc_list):
    print(f"Processing Directory {path}")
    if path.exists():
        print("in directory ---- " + path.relative_to(p).as_posix())
        for object in path.iterdir():
            if object.is_dir():
                # Recurse into directory
                if recurse_subdirectory_process:
                    # Process subdirectory with sub-process
                    print(f"Recurse Directory {object} with Process")
                    sema.acquire()
                    process = Process(target=multipartUpload, args=(
                        path, 
                        object.relative_to(folder).as_posix(), 
                        upload_manager, 
                        namespace))
                  
                    proc_list.append(process)
                    process.start()
                else:
                    # No separate process
                    print(f"Recurse Directory {object} in Thread")

                    processDirectory(
                        object, 
                        object_storage_client, 
                        upload_manager, 
                        namespace, 
                        proc_list
                    )
            else:
                # Must be a file
                print(f"Process File {object.as_posix()}")
                # If larger than 128MB, process as multi-part in separate process
                if os.stat(path).st_size > DEFAULT_PART_SIZE:
                    sema.acquire()
                    process = Process(target=multipartUpload, args=(
                        path, 
                        object.relative_to(folder).as_posix(), 
                        upload_manager, 
                        namespace))
                    proc_list.append(process)
                    process.start()
                else:
                    # Regular put (main thread)
                    object_name=object.relative_to(folder).as_posix()
                    print(f"Starting upload {object_name}")
                    start = time.time()
                    object_storage_client.put_object(
                        namespace, 
                        bucket_name, 
                        object_name=object_name, 
                        object_body=object
                    )
                    end = time.time()
                    print(f"Finished uploading {object_name} Time: {end - start}s")
                


if __name__ == '__main__':

    # Process Arguments
    if (len(sys.argv) < 2):
        print ("Not enough args")
        exit

    concurrency = sys.argv[1]
    folder = sys.argv[2]

    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    upload_manager = UploadManager(
        object_storage_client, allow_parallel_uploads=True, parallel_process_count=multipart_parallism)
    namespace = object_storage_client.get_namespace().data

    proc_list: array = []
    sema = Semaphore(concurrency)

    if folder.exists() and folder.is_dir():
        processDirectory(folder, object_storage_client,
                          upload_manager, namespace, proc_list)
    else:
        print ("Not a folder")
        exit

    for job in proc_list:
        job.join()
