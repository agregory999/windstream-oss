#! /usr/bin/env python3
'''
 This script allows for parallel processing of files from a directory tree on a local drive or mount to an OCI object storage bucket.

 NOTE: we are not doing any synchronization, nor can we do file filtering.  Both are potential options

 Access the script by running as such:
 $> python3 oss_upload.py <options>

 The only required options are:
 -c/--compartment   :   The OCID of the compartment where the bucket is located
 -f/--folder        :   The local folder to do the push from
 -b/--bucket        :   The name of the OCI OSS bucket within the compartment.  Assumes your user within OCI local config has write permissions

 Options:
 -p/--parallelism   :   Level of parallelism - # of processes to use 
 -w/--write         :   Add enclosing folder to the path.  For example, if the folder is /a/b/c, all files under c are uploaded, but c/file would be in the object name
 -th/--threshold    :   Size in bytes over which to favor multi-part upload instead of object PUT.   Defaults to 128M
 -v/--verbose       :   Prints more information
'''
import os
import time
import oci
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from multiprocessing.connection import wait
from oci.object_storage import UploadManager
from oci.object_storage.transfer.constants import DEFAULT_PART_SIZE
from pathlib import Path
import argparse

# The root directory path, Replace with your path
folder = None

# The Compartment OCID
compartment_id = ""

# The Bucket name where we will upload
bucket_name = ""

# Verbose
verbose = False

# Add Enclosing folder
add_enclosing_folder = False

# Concurrent processes
concurrency = 5

# MP threashold bytes
mp_threshold = DEFAULT_PART_SIZE


def progress_callback(bytes_uploaded):
    return
    #print("{} additional bytes uploaded".format(bytes_uploaded))

def stat_to_json(fp: str) -> dict:
    s_obj = os.stat(fp)
    return {k: str(getattr(s_obj, k)) for k in dir(s_obj) if k.startswith('st_')}

def uploadOSSProcess(path: str, filename: str, base_object_name: str, namespace, bucket_name, verbose: bool):

    # Initialize (don't like this)
    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    upload_manager = UploadManager(object_storage_client, allow_parallel_uploads=True)
    namespace = object_storage_client.get_namespace().data

    full_file_name = path + "/" + filename
    # If root directory object, just use filename as object name
    # otherwise take the relative path (base_object_name) and append / filename
    object_name = str(filename) if str(base_object_name) == "" else str(base_object_name) + "/" + str(filename)
    object_metadata = stat_to_json(full_file_name)
    object_metadata["mask"] = str(oct(os.stat(full_file_name).st_mode & 0o777))
    #object_metadata["size"] = str(os.stat(full_file_name).st_size)
    #object_metadata=os.stat(full_file_name).i
    if verbose:
        print(f"{os.getpid()} File Path: {full_file_name} Object Name: {object_name} File Size: {os.stat(full_file_name).st_size} Namespace: {namespace}")
    if os.stat(full_file_name).st_size > mp_threshold:
        start = time.time()
        
        # MP
        upload_manager.upload_file(
            namespace,
            bucket_name,
            object_name,
            full_file_name,
            metadata=object_metadata,
            part_size=DEFAULT_PART_SIZE,
            progress_callback=progress_callback)
        end = time.time()
        if verbose:
            print(f"{os.getpid()} Finished MP uploading {full_file_name} Time: {(end - start):.2f}s Size: {os.stat(full_file_name).st_size} bytes")
    else:
        with open(full_file_name, "rb") as in_file:
            # Reg put  
            start = time.time()
            object_storage_client.put_object(
                namespace_name=namespace, 
                bucket_name=bucket_name, 
                object_name=object_name, 
                put_object_body=in_file,
                opc_meta=object_metadata)
            end = time.time()
            if verbose:
                print(f"{os.getpid()} Finished PUT uploading {full_file_name} Time: {(end - start):.2f}s Size: {os.stat(full_file_name).st_size} bytes")

if __name__ == '__main__':

    # Parse Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("-w", "--write", help="take outer folder as enclosing and add it to object names", action="store_true")
    parser.add_argument("-b", "--bucket", help="name of bucket", required=True)
    parser.add_argument("-c", "--compartment", help="OCID of compartment", required=True)
    parser.add_argument("-p", "--parallelism", type=int, help="parallel processes allowed")
    parser.add_argument("-f", "--folder", type=Path, help="path to local folder to upload", required=True)
    parser.add_argument("-th", "--threshold", type=int, help="threshold in bytes for multi-part upload")
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

    # Threshold
    if args.threshold:
        mp_threshold = args.threshold

    print (f"**** Start - using {folder} with parallelism of {concurrency} and File threshold: {mp_threshold}.  Bucket name={bucket_name} ***")

    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    namespace = object_storage_client.get_namespace().data
 
    # Try with Process Pool
    with ProcessPoolExecutor(concurrency) as executor:
        # os.walk does recursive file tree walk and gives us paths 
        for (root,dirs,files) in os.walk(folder, topdown=True):
            # For each directory you get a tuple - files is a list within that tuple
            if verbose:
                print(f"OS Walk File: {root} / {files}")
            p = Path(root)
            #print(f"Path rel: {p}")
            rel_path = "" if str(p.relative_to(folder)) == "." else p.relative_to(folder)
            if verbose:
                print(f"Rel to {folder} : {rel_path}")
            # If enclosing folder, build object name with path element
            if add_enclosing_folder:
                base_object_name = os.path.basename(folder)
                # If we are in root folder, don't add anything to path.  Otherfile append relative path to file
                if str(rel_path) != "":
                    base_object_name += "/"
                    base_object_name += str(rel_path)
            else:
                base_object_name = str(rel_path)
            if verbose:
                print(f"Base Object Name :  {base_object_name}")
            # This is what takes the list of all files and sends it to the Process Pool
            # results is a Future List, but should be empty
            # pseudocode here:
            # For each file in the list, call the uploadOSS function, but use the folder name for each file.  Also the namespace and verbosity are passed into it.
            results = executor.map(uploadOSSProcess,repeat(root),files,repeat(base_object_name),repeat(namespace),repeat(bucket_name),repeat(verbose))
    for result in results:
        # This is only to make the program wait for all of the files to be processed.
        if verbose:
            print (f"Result {result}")

