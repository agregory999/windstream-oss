import array
import os
import time
import oci
from oci.object_storage import UploadManager
from oci.object_storage.transfer.constants import DEFAULT_PART_SIZE
from pathlib import Path

from multiprocessing import Semaphore
from multiprocessing import Process

# The root directory path, Replace with your path
p = Path('/home/andrew_gre/oracle-functions-samples')

# The Compartment OCID
compartment_id = "ocid1.compartment.oc1..aaaaaaaasczurylkzvviqfujhtinyiycg27yelt4opnwo3rgsfu22cuehv6q"

# The Bucket name where we will upload
#bucket_name = "bucket-directory-upload"
bucket_name = "directory-upload"


def progress_callback(bytes_uploaded):
    return
    #print("{} additional bytes uploaded".format(bytes_uploaded))


def uploadOSS(path: str, name: str, object_storage_client, namespace):
    with open(path, "rb") as in_file:
        #print(f"File Size: {os.stat(path).st_size}")
        if os.stat(path).st_size > 100000000:
            # Use multi-part
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
            print(f"{os.getpid()} Finished MP uploading: {name} Time: {end - start}s Size: {os.stat(path).st_size} bytes")
        else:
            print(f"{os.getpid()} Starting upload {name}")
            start = time.time()
            object_storage_client.put_object(
                namespace, bucket_name, name, in_file)
            end = time.time()
            print(f"{os.getpid()} Finished uploading {name} Time: {end - start}s Size: {os.stat(path).st_size} bytes")
    sema.release()

def processDirectoryLocal(path: Path, object_storage_client, namespace):
    print(f"{os.getpid()} Processing Directory {path}")
    if path.exists():
        for object in path.iterdir():
            print(f"{os.getpid()} Processing {object.as_posix()}")
            if object.is_dir():
                print(f"{os.getpid()} Recurse Directory {object.as_posix()}")
                processDirectoryLocal(object, object_storage_client, namespace)
            else:
                print(f"{os.getpid()} Process File {object.as_posix()} / {object.name}")
                # New Process
                sema.acquire()
                process = Process(target=uploadOSS, args=(
                        object.as_posix(), 
                        object.relative_to(p).as_posix(),
                        object_storage_client, 
                        namespace
                ))
                proc_list.append(process)
                process.start()
                #uploadOSS(object.as_posix(), object.relative_to(p).as_posix(), object_storage_client, namespace)


if __name__ == '__main__':

    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    namespace = object_storage_client.get_namespace().data
    sema = Semaphore(5)
    proc_list: array = []

    if p.exists() and p.is_dir():
        processDirectoryLocal(p, object_storage_client, namespace)

    for job in proc_list:
        job.join()
