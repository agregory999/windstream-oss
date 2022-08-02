#!/bin/sh

function usage(){
    echo This script performs a one-time backup of an FSS filesystem to OCI Object Storage.
    echo
    echo Pre-requisites that must be met before running this script:
    echo 1. rclone utility needs to be installed
    echo 2. rclone config file should exist
    echo
    echo Script needs to be run by root user or a sudoer like opc
    echo 'Usage: sh backup_fss_rclone.sh <FSS_mount_point> <Object_Bucket_name>'
}

# Exit on error
set -e

function parse_options(){
    while true; do
	if [ $# -lt 2 ]; then
            echo "Script requires FFS_mount_point. Refer usage below."
            echo 'Usage: sh backup_fss_rclone.sh <FSS_mount_point> <rclone remote> <Object_Bucket_name>'
            exit
        fi
        case "$1" in
            --help | -h)
                usage
                exit
                ;;
            -*)
                echo Unknown option: $1
                echo
                usage
                exit
                ;;
            *)
                break
                ;;
        esac
    done
}

function main() {
    parse_options "$@"

    # Set the variables
    fss_mount_point=$1
    snapshot_name="snap_`date '+%Y%b%d'`"
    RCLONE_REMOTE=$2
    oci_object_src_bucket=$3

    #Print the variables
    echo
    echo "Below are the variables used:"
    echo " FSS_mount_point		= $fss_mount_point"
    echo " snapshot_name 		= $snapshot_name"
    echo " oci_object_src_bucket	= $oci_object_src_bucket"
    echo " RCLONE_REMOTE	= $RCLONE_REMOTE"
    echo

    # Create a FSS snapshot
    echo "Step 1: Create a FSS snapshot"
    echo "Start: `date`"
    if [ -d $fss_mount_point ]; then
        if [ ! -d $fss_mount_point/.snapshot/$snapshot_name ]; then
            echo " time sudo mkdir $fss_mount_point/.snapshot/$snapshot_name"
            time sudo mkdir $fss_mount_point/.snapshot/$snapshot_name
        else
            echo "Snapshot exists."
        fi
    else
        echo "$fss_mount_point does not exist."
        exit
    fi
    echo "End: `date`"
    echo

    # Push the snapshot to object storage
    echo "Step 3: Push the snapshot to object storage"
    echo "Start: `date`"
    #echo "Make the Bucket path if it doesn't already exist"
    #echo "time rclone mkdir myobjectstorage:$oci_object_src_bucket/$snapshot_name"
    #time rclone mkdir myobjectstorage:$oci_object_src_bucket/$snapshot_name

    # Copy
    echo "time rclone copy --progress --transfers=`nproc` $fss_mount_point/.snapshot/$snapshot_name ${RCLONE_REMOTE}:/${oci_object_src_bucket}-snapshots/$snapshot_name"
    time rclone copy --progress --transfers=`nproc` $fss_mount_point/.snapshot/$snapshot_name ${RCLONE_REMOTE}:/${oci_object_src_bucket}-snapshots/$snapshot_name/

    # Sync - with versioning - just the share - no snap
    #echo "time rclone sync --progress --transfers=`nproc` $fss_mount_point ${RCLONE_REMOTE}:/${oci_object_src_bucket}-versioned --exclude .snapshot/**"
    #time rclone sync --progress --transfers=`nproc` $fss_mount_point ${RCLONE_REMOTE}:/${oci_object_src_bucket}-versioned --exclude ".snapshot/**"

    echo "End: `date`"
    echo
}

main "$@"


