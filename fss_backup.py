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
import sys

def extract_bytes(fs):
    try:
        # Also convert to int since update_time will be string.  When comparing
        # strings, "10" is smaller than "2".
        return int(fs.metered_bytes)
    except KeyError:
        return 0

# Define Snapshot name for FSS
# If daily, use the same name so that rclone sync will use versioning - ie incremental
# Weekly or monthly will copy to new folder later
snapshot_name = f"FSS-daily-Backup"

# File system temporary Mount Point
temp_mount = "/mnt/temp-backup"

# Backup Type
backup_type = None

# Dry Run
dry_run = False

# Sort Smallest to largest
sort_bytes = False

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
mt_ocid = None

# Threashold GB (Don't back up if > this)
threshold_gb = sys.maxsize

# Server-side copy (for weekly or monthly only)
# If set, uses rclone to avoid local copy, and uses last FSS-daily-Backup copy already on object store 
server_side_copy = None

fss_avail_domain = "UWQV:US-ASHBURN-AD-1"

# Number of cores (like nproc)
core_count = multiprocessing.cpu_count()

########### SUB ROUTINES ############################
def runRCLONE(rclone_remote, local_folder ):
    # Use subprocess
    return

def cleanupFileSnapshot(file_storage_client, fs_ocid):
    # Use API to attempt bucket creation
    snapshots = file_storage_client.list_snapshots(file_system_id=fs_ocid)
    for snap in snapshots.data:
        if snap.name == snapshot_name:
            if verbose:
                print(f"Deleting old Snapshot {snapshot_name} with OCID: {snap.id}")
            file_storage_client.delete_snapshot(snapshot_id=snap.id)
            if verbose:
                print(f"Sleeping 5sec to allow deletion to complete")
            time.sleep(5)
            return

def cleanupTemporaryMount():
    # Quietly ensures we have a clean mount point
    try:
        print(f"OS: umount -f {temp_mount}", flush=True)
        subprocess.run(["umount","-f",f"{temp_mount}"],shell=False, check=True)
    except:
        print(f"OS: umount failed but this is ok", flush=True)
        
def ensureTemporaryMount():
    # If mount doesn't exist
    if not os.path.isdir(temp_mount):
        # Attempt create and fail if we cannot
        try:
            os.makedirs(temp_mount)
        except:
            # Raise because if we cannot, we should kill the script immediately
            print(f"ERROR: Cannot create {temp_mount}")
            raise
      
def ensureBackupBucket(object_storage_client, bucket):
   # Check bucket status - create if necessary
    try:
        object_storage_client.get_bucket(namespace_name=namespace_name,bucket_name=bucket)
        if verbose:
            print(f"Bucket {bucket} found", flush=True)
    except oci.exceptions.ServiceError:
        if verbose:
            print(f"Bucket {bucket} not found - creating", flush=True)
        if not dry_run:
            object_storage_client.create_bucket(namespace_name=namespace_name,
                                                create_bucket_details = oci.object_storage.models.CreateBucketDetails(
                                                    name=bucket,
                                                    compartment_id=oss_compartment_ocid,
                                                    storage_tier="Standard",
                                                    object_events_enabled=True,
                                                    versioning="Enabled")
                                                )
        else:
            print(f"Dry Run: Would have created bucket {bucket} in compartment {oss_compartment_ocid}", flush=True)                                        

def getSuitableExport(file_storage_client, virtual_network_client, mt_ocid, fs_ocid):
    # Grab the list of exports from MT and iterate. Pick one with the right mount IP and return it
    mount_target = file_storage_client.get_mount_target(mount_target_id=mt_ocid)
    mount_ip = virtual_network_client.get_private_ip(private_ip_id=mount_target.data.private_ip_ids[0])
    
    print(f"MT IP: {mount_ip} ID {mount_target.data.id}",flush=True)
    # Grab from Export Set
    # export_set = file_storage_client.get_export_set(export_set_id=mount_target.export_set_id)
    
    # Iterate And grab first exports
    #exports = file_storage_client.list_exports(file_system_id=fs_ocid)
    exports = file_storage_client.list_exports(export_set_id=mount_target.data.export_set_id)
    for export in exports.data:
        if export.file_system_id == fs_ocid:
            print(f"MT {mount_ip.data.ip_address} Found {export.id} with path {export.path}",flush=True)
            return f"{mount_ip.data.ip_address}:{export.path}"
        print(f"No Match for {export.file_system_id}")
    # Nothing suitable
    raise NameError("Cannot find Matching export")

########### MAIN ROUTINE ############################    
# Main routine

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-fs", "--fssocid", help="FSS Compartment OCID of doing a single FS")
parser.add_argument("-fc", "--fsscompartment", help="FSS Compartment OCID", required=True)
parser.add_argument("-oc", "--osscompartment", help="OSS Backup Comaprtment OCID", required=True)
parser.add_argument("-r", "--remote", help="Named rclone remote for that user.  ie oci:", required=True)
parser.add_argument("-ad", "--availabilitydomain", help="AD for FSS usage.  Such as dDzb:US-ASHBURN-AD-1", required=True)
parser.add_argument("-m", "--mountocid", help="Mount Point OCID to use.", required=True)
parser.add_argument("-pr", "--profile", type=str, help="OCI Profile name (if not default)")
parser.add_argument("-ty", "--type", type=str, help="Type: daily(def), weekly, monthly", default="daily")
parser.add_argument("--dryrun", help="Dry Run - print what it would do", action="store_true")
parser.add_argument("-ssc","--serversidecopy", help="For weekly/monthly only - copies directly from latest daily backup, not source FSS", action="store_true")
parser.add_argument("-s","--sortbytes", help="Sort by byte size of FSS, smallest to largest (smaller FS backed up first", action="store_true")
parser.add_argument("-t","--threshold", help="GB threshold - do not back up share if more than this", type=int)
args = parser.parse_args()

# Process arguments
verbose = args.verbose
dry_run = args.dryrun
server_side_copy = args.serversidecopy
sort_bytes = args.sortbytes

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
if args.mountocid:
    mt_ocid = args.mountocid

# RCLONE Remote
if args.remote:
    rclone_remote = args.remote

# Type (daily, weekly, monthly)
if args.type:
    backup_type = args.type

# Availability Domain
if args.availabilitydomain:
    fss_avail_domain = args.availabilitydomain

# FSS Threshold
if args.threshold:
    threshold_gb = args.threshold

# Define OSS client and Namespace
if profile:
    config = oci.config.from_file(profile_name=profile)
else:
    config = oci.config.from_file()


########## STARTUP ######################

object_storage_client = oci.object_storage.ObjectStorageClient(config)
file_storage_client = oci.file_storage.FileStorageClient(config)
virtual_network_client = oci.core.VirtualNetworkClient(config)
namespace_name = object_storage_client.get_namespace().data

# Try to see if mount is there and clean - die if not (raise unchecked)
ensureTemporaryMount()
cleanupTemporaryMount()

# Explain what we are doing
if backup_type in ['weekly','monthly']:
    print(f'Performing Daily Incremental Backup AND {backup_type} using {"Server-Side Copy" if server_side_copy else "Rclone Copy"} method', flush=True)
else:
    print(f'Performing Daily Incremental Backup', flush=True)

# Print threshold if set
if threshold_gb < sys.maxsize:
    # This means it was set to anything
    print(f"GB Threshold set to {threshold_gb} GB - will skip any FS larger than this", flush=True)
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
    print(f'{f"Using {fss_ocid} in" if fss_ocid else "Iterating filesystems in"} Compartment: {fss_compartment_ocid}.  Count: {len(shares.data)}', flush=True)


# Sort by smallest to largest
if sort_bytes:
    print(f"Sorting FSS List smallest to largest", flush=True)
    shares.data.sort(key=extract_bytes)

for share in shares.data:
    print(f"Share name: {share.display_name} Size: {round(share.metered_bytes/(1024*1024*1024), 2)} GB", flush=True)
    backup_bucket_name = share.display_name.strip("/") + "_backup"
    
    if (share.metered_bytes > (threshold_gb * 1024 * 1024 * 1024)):
        print(f"File System is {round(share.metered_bytes/(1024*1024*1024), 2)} GB.  Threshold is {threshold_gb} GB.  Skipping", flush=True)
        continue

    # Ensure that the bucket is there
    ensureBackupBucket(object_storage_client=object_storage_client,bucket=backup_bucket_name)

    # Try to create Snap - if we can't, print error and continue
    try: 
        # FSS Snapshot (for clean backup)
        if not dry_run:
            # Try to delete FSS Snapshot - ok if it fails
            cleanupFileSnapshot(file_storage_client=file_storage_client, fs_ocid=share.id)

            if verbose:
                print(f"Creating FSS Snapshot: {snapshot_name} via API")
            snapstart = time.time()
            snapshot = file_storage_client.create_snapshot(create_snapshot_details=oci.file_storage.models.CreateSnapshotDetails(
                                                file_system_id=share.id,
                                                name=snapshot_name)
                                            )
            snapend = time.time()
            if verbose:
                print(f"FSS Snapshot time(ms): {(snapend - snapstart):.2f}s OCID: {snapshot.data.id}", flush=True)
        else:
            print(f"Dry Run: Create FSS Snapshot {snapshot_name} via API", flush=True)
    except:
        print(f"FSS SNAP ERROR - delete manually and retry this Share", flush=True)
        continue

    # Try mount and rclone, it not, clean up snapshot
    try:
        # Call the helper to get export path and mount
        # Get export path
        mount_path = getSuitableExport(file_storage_client, virtual_network_client, mt_ocid=mt_ocid, fs_ocid=share.id)
        if verbose:
            print(f"Using the following mount path: {mount_path}", flush=True)

        # Now call out to OS to mount RO
        if not dry_run:
            if verbose:
                print(f"OS: mount -r {mount_path} {temp_mount}", flush=True)
            subprocess.run(["mount","-r",f"{mount_IP}:{share.display_name}",f"{temp_mount}"],shell=False, check=True)
        else:
            print(f"Dry Run: mount -r {mount_path} {temp_mount}")

        # Define remote path on OSS
        remote_path = f"{rclone_remote}{backup_bucket_name}/{snapshot_name}"
        additional_copy_name = f"FSS-{backup_type}-Backup-{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        additional_remote_path = f"{rclone_remote}{backup_bucket_name}/{additional_copy_name}"
        
        if verbose:
            print(f"Using Remote Path (rclone_remote:bucket/snapshot): {rclone_remote}{backup_bucket_name}/{snapshot_name}", flush=True)

        # Call out to rclone it
        # Additional flags to consider
        # --s3-disable-checksum  only for large objects, avoid md5sum which is slow
        # --checkers = Core Count * 2
        if not dry_run:
            if verbose:
                print(f"Calling rclone with rclone sync --stats 5m -v --metadata --max-backlog 999999 --links --s3-chunk-size=16M --s3-upload-concurrency={core_count} --transfers={core_count} --checkers={core_count*2} /mnt/temp-backup/.snapshot/{snapshot_name} {remote_path}", flush=True)
            
            # Try / catch so as to not kill the process
            try:
                completed = subprocess.run(["rclone","sync", f'{"-vvv" if verbose else "-v"}', "--metadata", "--max-backlog", "999999", "--links",  
                                            "--s3-chunk-size=16M", "--stats", "5m", f"--s3-upload-concurrency={core_count}", f"--transfers={core_count}",f"--checkers={core_count*2}",
                                            f"/mnt/temp-backup/.snapshot/{snapshot_name}",f"{remote_path}"],shell=False, check=True)
                print (f"RCLONE output: {completed.stdout}", flush=True)
            except subprocess.CalledProcessError:
                print(f"RCLONE ERROR: Continue processing", flush=True)

            # Additional Backup if weekly or monthly selected.  Options are Direct Copy or Server Side Copy
            if backup_type in ['weekly','monthly']:
                if server_side_copy:
                    if verbose:
                        print(f'Creating additional {backup_type} backup called {additional_copy_name}. Implemented as rclone server side copy')
                        print(f"Calling rclone with rclone copy --stats 5m -v --no-check-dest--transfers={core_count*2} --checkers={core_count*2} {remote_path} {additional_remote_path}", flush=True)
                    # Try / catch so as to not kill the process
                    try:
                        # 2x transfers since server-side
                        # Also, since integrity check, don't check dest
                        completed = subprocess.run(["rclone","copy", "--stats", "5m", f'{"-vv" if verbose else "-v"}', "--no-check-dest", f"--transfers={core_count*2}",f"--checkers={core_count*2}",
                                                    f"{remote_path}", f"{additional_remote_path}"],shell=False, check=True)
                        print (f"RCLONE output: {completed.stdout}")
                    except subprocess.CalledProcessError:
                        print(f"RCLONE ERROR: Continue processing")
                else:
                    # Direct Copy
                    if verbose:
                        print(f'Creating additional {backup_type} backup called {additional_copy_name}. Implemented as rclone Direct Copy from FSS (full)')
                        print(f"Calling rclone with rclone sync --stats 5m -v --metadata --max-backlog 999999 --links --s3-chunk-size=16M --s3-upload-concurrency={core_count} --transfers={core_count} --checkers={core_count*2} /mnt/temp-backup/.snapshot/{snapshot_name} {additional_remote_path}", flush=True)
                    
                    # Try / catch so as to not kill the process
                    try:
                        # Still do integrity check (md5sum)
                        completed = subprocess.run(["rclone","copy", "--stats", "5m", f'{"-vv" if verbose else "-v"}', "--metadata", "--max-backlog", "999999", "--links",  
                                                    "--s3-chunk-size=16M", f"--s3-upload-concurrency={core_count}", f"--transfers={core_count}",f"--checkers={core_count*2}",
                                                    f"/mnt/temp-backup/.snapshot/{snapshot_name}",f"{additional_remote_path}"],shell=False, check=True)
                        print (f"RCLONE output: {completed.stdout}")
                    except subprocess.CalledProcessError:
                        print(f"RCLONE ERROR: Continue processing")

        else:
            if type in ['weekly','monthly']:
                if server_side_copy:
                    print(f"Dry Run: rclone copy -v {remote_path} {additional_remote_path}", flush=True)
                else:
                    print(f"Dry Run: rclone sync --progress --metadata --max-backlog 999999 --links --transfers={core_count} --checkers={core_count*2} /mnt/temp-backup/.snapshot/{snapshot_name} {remote_path}")

        # Unmount File System
        if not dry_run:
            if verbose:
                print(f"OS: umount /mnt/temp-backup", flush=True)
            subprocess.run(["umount","/mnt/temp-backup"],shell=False, check=True)
        else:
            print(f"Dry Run: umount /mnt/temp-backup", flush=True)

    except subprocess.CalledProcessError as exc:
        print(f"MOUNT ERROR: Continue processing to remove snapshot: {exc}", flush=True)
            
    # Delete Snapshot - no need to keep at this point
    if not dry_run:
        if verbose:
            print(f"Deleting Snapshot from FSS. Name: {snapshot.data.name} OCID:{snapshot.data.id}", flush=True)
        try:
            file_storage_client.delete_snapshot(snapshot_id=snapshot.data.id)
        except:
            print(f"Deletion of FSS Snapshot failed.  Please record OCID: {snapshot.data.id} and delete manually.", flush=True)    
    else:
        print(f"Dry Run: Delete Snapshot from FSS: {snapshot_name}")

end = time.time()
print(f"Finished | Time taken: {(end - start):.2f}s",flush=True)  


