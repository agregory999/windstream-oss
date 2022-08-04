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
import datetime
import oci
import os
import subprocess
import multiprocessing
import argparse

# Backup Type
backup_type = None

# Dry Run
dry_run = False

# The Compartment OCID for FSS shares
fss_compartment_ocid = None

# The Compartment OCID for specific FSS share (if set)
fss_ocid = None

# The OSS Bucket Compartment OCID where we will upload
oss_compartment_ocid = None

# RCLONE Remote (Create via OS)
rclone_remote = None

# Mount Point IP
mount_IP = None


fss_avail_domain = "UWQV:US-ASHBURN-AD-1"

# Number of cores (like nproc)
core_count = multiprocessing.cpu_count()

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
parser.add_argument("-fs", "--fssocid", help="FSS Compartment OCID of doing a single FS")
parser.add_argument("-fc", "--fsscompartment", help="FSS Compartment OCID", required=True)
parser.add_argument("-oc", "--osscompartment", help="OSS Backup Comaprtment OCID", required=True)
parser.add_argument("-r", "--remote", help="Named rclone remote for that user.  ie oci:", required=True)
parser.add_argument("-ad", "--availabilitydomain", help="AD for FSS usage.  Such as dDzb:US-ASHBURN-AD-1", required=True)
parser.add_argument("-m", "--mountip", help="Mount Point IP to use.", required=True)
parser.add_argument("-pr", "--profile", type=str, help="OCI Profile name (if not default)")
parser.add_argument("-ty", "--type", type=str, help="Type: daily(def), weekly, monthly", default="daily")
parser.add_argument("--dryrun", help="Dry Run - print what it would do", action="store_true")
args = parser.parse_args()

# Process arguments
verbose = args.verbose
dry_run = args.dryrun

# Default(None) or named
profile = args.profile

# FSS Compartment OCID
if args.fsscompartment:
    fss_compartment_ocid = args.fsscompartment

# FSS Single OCID
if args.fssocid:
    fss_ocid = args.fssocid

# OSS Compartment OCID
if args.osscompartment:
    oss_compartment_ocid = args.osscompartment

# Mount IP
if args.mountip:
    mount_IP = args.mountip

# RCLONE Remote
if args.remote:
    rclone_remote = args.remote

# Type (daily, weekly, monthly)
if args.type:
    backup_type = args.type

# Availability Domain
if args.availabilitydomain:
    fss_avail_domain = args.availabilitydomain

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
# If daily, use the same name so that rclone sync will use versioning - ie incremental
# Weekly or monthly will create a new snapshot folder and thus it will be new
if backup_type == "daily":
    snapshot_name = f"FSS-{backup_type}-Backup"
    print("Using daily incremental backup")
else:
    snapshot_name = f"FSS-{backup_type}-Backup-{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    print(f"Using weekly/monthly incremental backup called: {snapshot_name}")

start = time.time()
# Main loop - list File Shares

# For listing, if the fss_ocid is set to a single FS, only do that in the filter
# Else get all shares
if fss_ocid:
    shares = file_storage_client.list_file_systems(compartment_id=fss_compartment_ocid, 
                                                    id=fss_ocid,
                                                    availability_domain=fss_avail_domain,
                                                    lifecycle_state="ACTIVE")
else:
    shares = file_storage_client.list_file_systems(compartment_id=fss_compartment_ocid, 
                                                    availability_domain=fss_avail_domain,
                                                    lifecycle_state="ACTIVE")

# At this point iterate the list (even if single)
if verbose:
    print(f"Iterating filesystems.  Count: {len(shares.data)}")

for share in shares.data:
    print(f"Share name: {share.display_name}")
    backup_bucket_name = share.display_name.strip("/") + "_backup"
    
    # Check bucket status - create if necessary
    try:
        object_storage_client.get_bucket(namespace_name=namespace_name,bucket_name=backup_bucket_name)
        if verbose:
            print(f"Bucket {backup_bucket_name} found")
    except oci.exceptions.ServiceError:
        if verbose:
            print(f"Bucket {backup_bucket_name} not found - creating")
        if not dry_run:
            object_storage_client.create_bucket(namespace_name=namespace_name,
                                                create_bucket_details = oci.object_storage.models.CreateBucketDetails(
                                                    name=backup_bucket_name,
                                                    compartment_id=oss_compartment_ocid,
                                                    storage_tier="Standard",
                                                    object_events_enabled=True,
                                                    versioning="Enabled")
                                                )
        else:
            print(f"Dry Run: Would have created bucket {backup_bucket_name} in compartment {oss_compartment_ocid}")                                        
        
    # FSS Snapshot (for clean backup)
    if not dry_run:
        if verbose:
            print(f"Creating FSS Snapshot: {snapshot_name} via API")
        snapstart = time.time()
        snapshot = file_storage_client.create_snapshot(create_snapshot_details=oci.file_storage.models.CreateSnapshotDetails(
                                            file_system_id=share.id,
                                            name=snapshot_name)
                                        )
        snapend = time.time()
        if verbose:
            print(f"FSS Snapshot time(ms): {(snapend - snapstart):.2f}s OCID: {snapshot.data.id}")
    else:
        print(f"Dry Run: Create FSS Snapshot {snapshot_name} via API")

    # Now call out to OS to mount RO
    if not dry_run:
        if verbose:
            print(f"OS: mount -r {mount_IP}:{share.display_name} /mnt/temp-backup")
        subprocess.run(["mount","-r",f"{mount_IP}:{share.display_name}","/mnt/temp-backup"],shell=False, check=True)
    else:
        print(f"Dry Run: mount -r {mount_IP}:{share.display_name} /mnt/temp-backup")

    # Define remote path on OSS
    remote_path = f"{rclone_remote}{backup_bucket_name}/{snapshot_name}"
    if verbose:
        print(f"Using Remote Path (rclone_remote:bucket/snapshot): {rclone_remote}{backup_bucket_name}/{snapshot_name}")

    # Call out to rclone it
    if not dry_run:
        if verbose:
            print(f"Calling rclone with rclone sync --progress --metadata --max-backlog 999999 --links --transfers={core_count} --checkers={core_count*2} /mnt/temp-backup/.snapshot/{snapshot_name} {remote_path}")
        
        # Try / catch so as to not kill the process
        try:
            subprocess.run(["rclone","sync","--progress","--metadata", "--max-backlog", "999999", "--links",f"--transfers={core_count}",f"--checkers={core_count*2}",f"/mnt/temp-backup/.snapshot/{snapshot_name}",f"{remote_path}"],shell=False, check=True)
        except subprocess.CalledProcessError:
            print(f"RCLONE ERROR: Continue processing")
    else:
        print(f"Dry Run: rclone sync --progress --metadata --max-backlog 999999 --links --transfers={core_count} --checkers={core_count*2} /mnt/temp-backup/.snapshot/{snapshot_name} {remote_path}")

    # Delete Snapshot (not necessary)
    # If snapshot is deleted, the .snapshot will not be included in the permissions file. 
    if not dry_run:
        if verbose:
            print(f"Deleting Snapshot from FSS. Name: {snapshot.data.name} OCID:{snapshot.data.id}")
        try:
            file_storage_client.delete_snapshot(snapshot_id=snapshot.data.id)
        except:
            print(f"Deletion of FSS Snapshot failed.  Please record OCID: {snapshot.data.id} and delete manually.")    
    else:
        print(f"Dry Run: Delete Snapshot from FSS: {snapshot_name}")

    # Unmount Snapshot File System
    if not dry_run:
        if verbose:
            print(f"OS: umount /mnt/temp-backup/{snapshot_name}")
        subprocess.run(["umount",f"/mnt/temp-backup/{snapshot_name}"],shell=False, check=True)
    else:
        print(f"Dry Run: umount /mnt/temp-backup/{snapshot_name}")

    # Save Permissions
    # Creates a file in the object folder with all permissions - this can be used to restore ACL later
    if not dry_run:
        try:
            if verbose:
                print(f"Creating permissions file: /tmp/.{snapshot_name}-permissions.facl")
            with open(f"/tmp/.{snapshot_name}-permissions.facl", "w") as outfile:
                subprocess.run(["getfacl","-p","-R",f"/mnt/temp-backup"],shell=False, check=True, stdout=outfile, stderr=subprocess.STDOUT)
            subprocess.run(["rclone","copy","--progress",f"/tmp/.{snapshot_name}-permissions.facl",f"{remote_path}"],shell=False, check=True)
        except subprocess.CalledProcessError as exc:
            print("Status : FAIL", exc.returncode, exc.output)
    else:
        print(f"Dry Run: Create permissions file /tmp/.{snapshot_name}-permissions.facl")
        print(f"Dry Run: rclone copy permissions file to {remote_path}")

    # Unmount File System
    if not dry_run:
        if verbose:
            print(f"OS: umount /mnt/temp-backup")
        subprocess.run(["umount","/mnt/temp-backup"],shell=False, check=True)
    else:
        print(f"Dry Run: umount /mnt/temp-backup")

end = time.time()
print(f"Finished | Time taken: {(end - start):.2f}s",flush=True)  
