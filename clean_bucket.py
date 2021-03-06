#! /usr/bin/env python3

'''
Cleans out an OSS bucket
No regard for what is in it
Operates with parallelism 
'''

import time
import os
import oci
import os

import argparse
import concurrent.futures
import threading

# The Compartment OCID
compartment_id = None

# The Bucket name where we will upload
bucket_name = None


def deleteObject(object_storage_client,namespace_name,bucket_name,object_name ):
    #print(f"{threading.get_ident()} | Delete: {object_name}")
    object_storage_client.delete_object(
        namespace_name=namespace_name,
        bucket_name=bucket_name,
        object_name=object_name
    )

# Main routine

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-b", "--bucket", help="name of bucket", required=True)
parser.add_argument("-p", "--parallelism", type=int, help="parallel processes allowed", default=5)
args = parser.parse_args()

# Process arguments
verbose = args.verbose

# Bucket Name
if args.bucket:
    bucket_name = args.bucket

# Parallelism
concurrency = args.parallelism

# Define OSS client and Namespace
config = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(config)
namespace_name = object_storage_client.get_namespace().data



# Main loop - delete objects, then if more, repeat on that until gone
with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
    futures = []

    obj_count = 0
    iteration = 0
    start = time.time()
    more_to_do = True
    while more_to_do:
        print(f"{os.getpid()} Iteration: {iteration}")
        # Initial bucket List - response element count limited by API
        list_objects_response = object_storage_client.list_objects(
            namespace_name=object_storage_client.get_namespace().data,
            bucket_name=bucket_name
        )

        for obj in list_objects_response.data.objects:
            if verbose:
                print(f"{os.getpid()} Iter: {iteration} | Object Delete: {obj.name}")
            futures.append(executor.submit(deleteObject, object_storage_client,namespace_name,bucket_name,obj.name))
            obj_count += 1
        iteration += 1
        # Ternary - to control loop
        more_to_do = True if list_objects_response.data.next_start_with != None else False
        print(f"Control data: {list_objects_response.data.next_start_with} | Var: {more_to_do}")
        # while list_objects_response.data.next_start_with != None:
        #     print(f"{os.getpid()} Performing another iteration of List/delete: {list_objects_response.data.next_start_with}")
        #     list_objects_response2 = object_storage_client.list_objects(
        #         namespace_name=namespace_name,
        #         bucket_name=bucket_name,
        #         start=list_objects_response.data.next_start_with
        #     )  

        #     for obj in list_objects_response2.data.objects:
        #         print(f"{os.getpid()} Object Delete: {obj.name}")
        #         futures.append(executor.submit(deleteObject, object_storage_client,namespace_name,bucket_name,obj.name))
        #         count += 1
    end = time.time()
for result in futures:
    if verbose:
        print (f"Result {result}")
if verbose:
    print(f"{os.getpid()} Count of deleted objects: {obj_count}  | Time taken: {(end - start):.2f}s",flush=True)  
