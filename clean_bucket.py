import time
import os
import sys
import oci
import os

from multiprocessing import Semaphore
from multiprocessing import Process
import concurrent.futures
import threading

# The Compartment OCID
compartment_id = "ocid1.compartment.oc1..aaaaaaaasczurylkzvviqfujhtinyiycg27yelt4opnwo3rgsfu22cuehv6q"

# The Bucket name where we will upload
bucket_name = "directory-upload"

# Define client
config = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(config)

# Namespace
namespace_name = object_storage_client.get_namespace().data

# Concurrency
sema = Semaphore(20)

def deleteObject(object_storage_client,namespace_name,bucket_name,object_name ):
    print(f"{threading.get_ident()} | Delete: {object_name}")
    object_storage_client.delete_object(
        namespace_name=namespace_name,
        bucket_name=bucket_name,
        object_name=object_name
    )
    sema.release()

# List / clean
list_objects_response = object_storage_client.list_objects(
    namespace_name=object_storage_client.get_namespace().data,
    bucket_name=bucket_name
)

# Main loop
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = []

    for obj in list_objects_response.data.objects:
        print(f"{os.getpid()} Object Name: {obj.name}")
        futures.append(executor.submit(deleteObject, object_storage_client,namespace_name,bucket_name,obj.name))


    while list_objects_response.data.next_start_with != None:
        print(f"{os.getpid()} Next round {list_objects_response.data.next_start_with}")
        list_objects_response2 = object_storage_client.list_objects(
            namespace_name=namespace_name,
            bucket_name=bucket_name,
            start=list_objects_response.data.next_start_with
        )  

        for obj in list_objects_response2.data.objects:
            print(f"{os.getpid()} (list_objects_response.data.next_start_with) Object Name: {obj.name}")
            futures.append(executor.submit(deleteObject, object_storage_client,namespace_name,bucket_name,obj.name))
    
    concurrent.futures.wait(futures)
    executor.shutdown()