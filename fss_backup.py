#! /usr/bin/env python3

'''
Iterate FSS Shares using OCI API
For each share
    perform snapshot
    mount r/o
    call out to rclone
    unmount
    fire event (nice to have)
'''

import time
import oci
import os
import subprocess

import argparse

# The Compartment OCID for FSS shares
fss_compartment_ocid = None

# The OSS Bucket Compartment OCID where we will upload
oss_compartment_ocid = None

# RCLONE Remote (Create via OS)
rclone_remote = None

def runRCLONE(rclone_remote, local_folder ):
    # Use subprocess
    return

def createBackupBucket(object_storage_client, share_name):
    # Use API to attempt bucket creation
    object_storage_client
    return

#######################################    
# Main routine

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-fc", "--fsscompartment", help="FSS Compartment OCID", required=True)
parser.add_argument("-oc", "--osscompartment", help="OSS Backup Comaprtment OCID", required=True)
parser.add_argument("-r", "--remote", help="Named rclone remote for that user.  ie oci:", required=True)
parser.add_argument("-pr", "--profile", type=str, help="OCI Profile name (if not default)")
args = parser.parse_args()

# Process arguments
verbose = args.verbose

# Default(None) or named
profile = args.profile

# FSS Compartment OCID
if args.fsscompartment:
    fss_compartment_ocid = args.fsscompartment

# OSS Compartment OCID
if args.osscompartment:
    oss_compartment_ocid = args.osscompartment

# RCLONE Remote
if args.remote:
    rclone_remote = args.remote

# Define OSS client and Namespace
if profile:
    config = oci.config.from_file(profile_name=profile)
else:
    config = oci.config.from_file()


##################################

object_storage_client = oci.object_storage.ObjectStorageClient(config)
file_storage_client = oci.file_storage.FileStorageClient(config)
namespace_name = object_storage_client.get_namespace().data

# Define Snapshot name for FSS
snapshot_name = f"FSS-Backup-{time.time()}"

start = time.time()
# Main loop - list File Shares

shares = file_storage_client.list_file_systems(compartment_id=fss_compartment_ocid, 
                                                availability_domain="UWQV:US-ASHBURN-AD-3",
                                                lifecycle_state="ACTIVE")
for share in shares.data:
    print(f"Share name: {share.display_name}")
    backup_bucket_name = share.display_name + "_backup"
    
    # Check bucket status - create if necessary
    try:
        object_storage_client.get_bucket(namespace_name=namespace_name,bucket_name=backup_bucket_name)
        print("Bucket found")
    except oci.exceptions.ServiceError:
        print(f"Bucket not found - creating")
        object_storage_client.create_bucket(namespace_name=namespace_name,
                                            create_bucket_details = oci.object_storage.models.CreateBucketDetails(
                                                name=backup_bucket_name,
                                                compartment_id=oss_compartment_ocid,
                                                storage_tier="Standard",
                                                object_events_enabled=True,
                                                auto_tiering="InfrequentAccess")
                                            )
    # FSS Snapshot (for clean backup)
    file_storage_client.create_snapshot(create_snapshot_details=oci.file_storage.models.CreateSnapshotDetails(
                                            file_system_id=share.id,
                                            name=snapshot_name)
                                        )
    # Now call out to OS to mount RO
# root

    # Call out to rclone it
    remote_path = f"{rclone_remote}{backup_bucket_name}/{snapshot_name}"
    subprocess.run(["rclone","copy","--progress","--transfers=2","/Users/argregor/windstream-oss",f"{remote_path}"],shell=False, check=True)

    # Unmount it
# root

end = time.time()
if verbose:
    print(f"Finished | Time taken: {(end - start):.2f}s",flush=True)  
