import array
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from multiprocessing.connection import wait
import os
from pickle import FALSE
import time
import oci
from oci.object_storage import UploadManager
from oci.object_storage.transfer.constants import DEFAULT_PART_SIZE
from pathlib import Path

from multiprocessing import Semaphore
from multiprocessing import Process
import argparse
import threading

# The root directory path, Replace with your path
folder = Path('/Users/argregor/windstream-python/files')

# The Compartment OCID
compartment_id = "ocid1.compartment.oc1..aaaaaaaasczurylkzvviqfujhtinyiycg27yelt4opnwo3rgsfu22cuehv6q"

# The Bucket name where we will upload
#bucket_name = "bucket-directory-upload"
bucket_name = "directory-upload"

# Verbose
verbose = False

# Add Enclosing folder
add_enclosing_folder = False

# Concurrent processes
concurrency = 5

# MP threashold bytes
mp_threshold = 100000000

def progress_callback(bytes_uploaded):
    return
    #print("{} additional bytes uploaded".format(bytes_uploaded))

def uploadOSSProcess(path: str, filename: str, base_object_name: str, namespace):

    data = threading.local()

    # Initialize (don't like this)
    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    upload_manager = UploadManager(object_storage_client, allow_parallel_uploads=True)
    namespace = object_storage_client.get_namespace().data

    full_file_name = path + "/" + filename
    print(f"{os.getpid()} Path: {full_file_name} Name: {base_object_name} Size: {os.stat(full_file_name).st_size} Namespace: {namespace}")

    object_name = str(base_object_name) + "/" + str(filename)
    if os.stat(full_file_name).st_size > mp_threshold:
        data.start = time.time()

        # MP
        upload_manager.upload_file(
            namespace,
            bucket_name,
            object_name,
            full_file_name,
            part_size=DEFAULT_PART_SIZE,
            progress_callback=progress_callback)
        data.end = time.time()
        print(f"{os.getpid()} Finished MP uploading {full_file_name} Time: {(data.end - data.start):.2f}s Size: {os.stat(full_file_name).st_size} bytes")
    else:
        with open(full_file_name, "rb") as in_file:
            # Reg put  
            data.start = time.time()
            object_storage_client.put_object(
                namespace, bucket_name, object_name, in_file)
            data.end = time.time()
            print(f"{os.getpid()} Finished uploading {full_file_name} Time: {(data.end - data.start):.2f}s Size: {os.stat(full_file_name).st_size} bytes")

def uploadOSS(path: str, name: str, object_storage_client, namespace):
    with open(path, "rb") as in_file:
        #print(f"File Size: {os.stat(path).st_size}")
        if os.stat(path).st_size > 100000000:
            # Use multi-part
            if verbose:
                print(f"{os.getpid()} Starting MP multi-part upload {name}")

            upload_manager = UploadManager(
                object_storage_client, allow_parallel_uploads=True)
            start = time.time()
            response = upload_manager.upload_file(
                namespace,
                bucket_name,
                name,
                path,
                part_size=DEFAULT_PART_SIZE,
                progress_callback=progress_callback)
            end = time.time()
            if verbose:
                print(f"{os.getpid()} Finished MP uploading: {name} Time: {end - start}s Size: {os.stat(path)} bytes")
        else:
            if verbose:
                print(f"{os.getpid()} Starting upload {path} {name}")
            start = time.time()
            object_storage_client.put_object(
                namespace, bucket_name, name, in_file)
            end = time.time()
            if verbose:
                print(f"{os.getpid()} Finished uploading {name} Time: {end - start}s Size: {os.stat(path)} bytes")


def processDirectoryLocal(path: Path, object_storage_client, namespace):
    print(f"{os.getpid()} Processing Directory {path}")
    
    if path.exists():
        for object in path.iterdir():
            #print(f"{os.getpid()} Processing {object.as_posix()}")
            if object.is_dir():
                if verbose:
                    print(f"{os.getpid()} Recurse Directory {object.as_posix()}")
                processDirectoryLocal(object, object_storage_client, namespace)
            else:
                # File
                object_name = os.path.basename(folder) + "/" + object.relative_to(folder).as_posix() if add_enclosing_folder else object.relative_to(folder).as_posix()

                # print(f"os.path.basename: {os.path.basename(folder)}")
                # print(f"Using object_name {object_name}")
                if verbose:
                    print(f"{os.getpid()} Process File {object.relative_to(folder).as_posix()} - Insert as name: {object_name}")
                # New Process
                sema.acquire()
                process = Process(target=uploadOSS, args=(
                        object.as_posix(), 
                        object_name,
                        object_storage_client, 
                        namespace
                ))
                proc_list.append(process)
                process.start()
                #uploadOSS(object.as_posix(), object.relative_to(folder).as_posix(), object_storage_client, namespace)



if __name__ == '__main__':

    # Parse Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("-w", "--write", help="take outer folder as enclosing and add it to object names", action="store_true")
    parser.add_argument("-b", "--bucket", help="name of bucket")
    parser.add_argument("-c", "--compartment", help="OCID of compartment")
    parser.add_argument("-p", "--parallelism", type=int, help="parallel processes allowed")
    parser.add_argument("-f", "--folder", type=Path, help="path to local folder to upload", required=True)
    args = parser.parse_args()


    # Verbose or not
    verbose = args.verbose
    add_enclosing_folder = args.write

    
    # Bucket Name
    if args.bucket:
        bucket_name = args.bucket
    
    # Compartment
    if args.compartment:
        compartment_id = args.compartment
    
    # Folder
    if args.folder:
        folder = args.folder

    # Parallelism
    if args.parallelism:
        concurrency = args.parallelism

    print (f"**** Start - using {folder} with parallelism of {concurrency}.  Bucket name={bucket_name} ***")

    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    namespace = object_storage_client.get_namespace().data
 
    # Try with Process Pool
    futures = []
    with ProcessPoolExecutor(concurrency) as executor:
        for (root,dirs,files) in os.walk(folder, topdown=True):
            print(f"OS Walk Root: {root}")
            print(f"OS Walk File: {files}")
            p = Path(root)
            base_object_name = os.path.basename(folder) + "/" + str(p.relative_to(folder)) if add_enclosing_folder else p.relative_to(folder)
            #object_name = os.path.basename(folder) + "/" + object.relative_to(folder).as_posix() if add_enclosing_folder else object.relative_to(folder).as_posix()
            results = executor.map(uploadOSSProcess,repeat(root),files,repeat(base_object_name),repeat(namespace))
    for result in results:
        print(result)

    exit
    # config = oci.config.from_file()
    # object_storage_client = oci.object_storage.ObjectStorageClient(config)
    # namespace = object_storage_client.get_namespace().data
    # sema = Semaphore(concurrency)
    # proc_list: array = []

    # if folder.exists() and folder.is_dir():
    #     processDirectoryLocal(folder, object_storage_client, namespace)

    # for job in proc_list:
    #     job.join()