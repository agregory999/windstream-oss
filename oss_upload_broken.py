import time
import os
import sys
from xml.dom.expatbuilder import Namespaces
import oci
import os
from oci.object_storage import UploadManager
from oci.object_storage import object_storage_client
from oci.object_storage.transfer.constants import DEFAULT_PART_SIZE
from array import array
from pathlib import Path

from multiprocessing import Semaphore
from multiprocessing import Process
import concurrent.futures
import threading
from concurrent.futures import Executor, wait

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
recurse_subdirectory_process = True

# Multi-part Parallelism
multipart_parallism = 5

# upload_manager = None
# object_storage_client = None

def progress_callback(bytes_uploaded):
    return
    #print("{} additional bytes uploaded".format(bytes_uploaded))

def initializer_worker():
    name = threading.current_thread().name
    # data = threading.local()
    # data.object_storage_client = oci.object_storage.ObjectStorageClient(config)
    # data.upload_manager = UploadManager(
    #     object_storage_client, allow_parallel_uploads=True, parallel_process_count=1)
    print(f'Initializing worker thread {name}')

def multipartUpload(path: str, name: str, namespace):
    config = oci.config.from_file()
    data = threading.local()
    data.object_storage_client = oci.object_storage.ObjectStorageClient(config)
    data.upload_manager = UploadManager(
       object_storage_client, allow_parallel_uploads=True, parallel_process_count=1)
    namespace = data.object_storage_client.get_namespace().data

    with open(path, "rb") as in_file:
        start = time.time()
        # Actual uplaod as multi-part
        print(f"Started MP uploading: {name}")
        response = data.upload_manager.upload_file(
                namespace,
                bucket_name,
                name,
                path,
                part_size=int(DEFAULT_PART_SIZE),
                progress_callback=progress_callback)

        end = time.time()
        print(f"Finished MP uploading: {name} Time: {end - start}s")

def regularUpload(path: str, name: str, object_storage_client, namespace):
    config = oci.config.from_file()
    data = threading.local()
    data.object_storage_client = oci.object_storage.ObjectStorageClient(config)
    namespace = data.object_storage_client.get_namespace().data

    # Actual uplaod as multi-part
    start = time.time()
    print(f"TID {threading.get_ident()} Started Reg uploading: {name}")
    with open(object, "rb") as in_file:
        data.object_storage_client.put_object(
        namespace, 
        bucket_name, 
        object_name=name, 
        put_object_body=in_file
    )
    end = time.time()
    print(f"TID {threading.get_ident()} Finished Reg uploading: {name} Time: {end - start}s")

# def processDirectory(path: Path, namespace, object_storage_client, upload_manager, proc_list, recurse_level: int):
#     print(f"TID {threading.get_ident()} Processing Directory {path}")
#     if path.exists():
#         print(f"{recurse_level}|Directory {path.relative_to(folder).as_posix()} ")
#         for object in path.iterdir():
#             if object.is_dir():
#                 # Recurse into directory
#                 if recurse_subdirectory_process:
#                     # Process subdirectory with sub-process
#                     print(f"Recurse Directory {object} with Process")
#                     sema.acquire()
#                     process = Process(target=processDirectory, args=(
#                         object, 
#                         namespace,
#                         object_storage_client, 
#                         upload_manager, 
#                         proc_list,
#                         recurse_level+1
#                         )
#                     )
                  
#                     proc_list.append(process)
#                     process.start()
#                 else:
#                     # No separate process
#                     print(f"TID {threading.get_ident()} Recurse Directory {object} in Thread")
#                     processDirectory(
#                         object, 
#                         namespace, 
#                         object_storage_client,
#                         upload_manager, 
#                         proc_list, 
#                         recurse_level+1
#                     )
#             else:
#                 # Must be a file
#                 print(f"TID {threading.get_ident()} Object: {object.as_posix()} File size: {os.stat(path).st_size} Threshold: {DEFAULT_PART_SIZE}")
#                 # If larger than 128MB, process as multi-part in separate process
#                 if os.stat(object).st_size > DEFAULT_PART_SIZE:
#                     # sema.acquire()
#                     # process = Process(target=multipartUpload, args=(
#                     #     object.as_posix(), 
#                     #     object.relative_to(folder).as_posix(), 
#                     #     upload_manager, 
#                     #     namespace))
#                     # proc_list.append(process)
#                     # process.start()
#                     executor.map(multipartUpload, object.as_posix(), object.relative_to(folder).as_posix(),namespace)
#                     #futures.append(executor.submit(multipartUpload, object.as_posix(), object.relative_to(folder).as_posix(),namespace))
#                 else:
#                     # Regular put (main thread)
#                     object_name=object.relative_to(folder).as_posix()
#                     # print(f"{os.getpid()} Starting upload {object_name}")
#                     # start = time.time()
#                     # with open(object, "rb") as in_file:
#                     #     object_storage_client.put_object(
#                     #         namespace, 
#                     #         bucket_name, 
#                     #         object_name=object_name, 
#                     #         put_object_body=in_file
#                     # )
#                     # end = time.time()
#                     # print(f"{os.getpid()} Finished uploading {object_name} Time: {end - start}s")
#                     executor.map(regularUpload, object.as_posix(), object.relative_to(folder).as_posix(),namespace)
#                     #futures.append(executor.submit(regularUpload, object.as_posix(), object.relative_to(folder).as_posix(),namespace))
#     return futures


# Process Arguments
if (len(sys.argv) < 2):
    print ("Not enough args")
    exit

concurrency = int(sys.argv[1])
folder = Path(sys.argv[2])

config = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(config)
upload_manager = UploadManager(
    object_storage_client, allow_parallel_uploads=True, parallel_process_count=1)
namespace = object_storage_client.get_namespace().data

proc_list: array = []
sema = Semaphore(concurrency)

futures = []
with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
    print(f"Directory {folder} ")
    for object in folder.iterdir():
        if object.is_dir():
            # Kick off another one of these as a process on the sub-dir
            # sema.acquire()
            # process = Process(target=__main__, args=(
            #         object, 
            #         namespace,
            #         object_storage_client, 
            #         upload_manager, 
            #         proc_list,
            #         recurse_level+1
            #         )
            #     )
                
            #     proc_list.append(process)
            #     process.start()
            print(f"Directory {folder} - kick off")
        else: 
            # Fire off thread
            futures.append(executor.submit(regularUpload, object.as_posix(), object.relative_to(folder).as_posix(),namespace))
    wait()

# if folder.exists() and folder.is_dir():
#     processDirectory(
#         folder, 
#         namespace, 
#         futures,
#         executor,
#         0)
# else:
#     print ("Not a folder")
#     exit


    # wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
    # print(f"All Threads Complete")
    # time.sleep(10)
    # executor.shutdown()
