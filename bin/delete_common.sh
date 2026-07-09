#!/usr/bin/env bash

dir1="/discover/nobackup/jardizzo/aerosols/TropOMI/"
dir2="/discover/nobackup/hzafar/ghgc/CDSE-tropomi/eodag_workspace_download/"
filelist="logs/common_items.txt"

# Function to inspect differences
inspect_common_and_diff() {
  diff -q "$1" "$2"| grep -v "^Only in $2" | sort > logs/common.log
  diff -q "$1" "$2"| grep -v "^Common subdirectories" | sort > logs/diff.log
}

# Function to delete common from my directory for space
delete_common_files() {
  # Create a list of files to delete
  comm -12 <(ls -1 $dir1 | sort) <(ls -1 $dir2 | sort) > $filelist

  # Confirm delete
  count=$(wc -l < $filelist)
  read -p "Found $count common items. Do you want to delete them from $dir2? (y/n): " confirm

  if [[ "$confirm" =~ ^[Yy]$ ]]; then
      cat $filelist | xargs -I {} rm -rf $dir2/{}
      echo "Items deleted successfully."
  else
      echo "Deletion cancelled."
  fi
}

# inspect_common_and_diff $dir1 $dir2
delete_common_files $dir1 $dir2

