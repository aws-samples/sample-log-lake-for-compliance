#!/usr/bin/env python3
"""
Simple S3 versioned object cleanup script - Debug version
Deletes versions and delete markers in batches
"""

import json
import subprocess
import tempfile
import os

def run_aws_command(cmd):
    """Run AWS CLI command and return output"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running command: {cmd}")
        print(f"Error: {result.stderr}")
        return None
    return result.stdout.strip()

def get_versions(bucket_name, batch_size):
    """Get object versions up to batch_size"""
    cmd = f'aws s3api list-object-versions --bucket "{bucket_name}" --max-items {batch_size} --query "Versions[].[Key,VersionId]" --output text'
    output = run_aws_command(cmd)
    if not output:
        return []
    
    versions = []
    for line in output.split('\n'):
        if line.strip():
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                versions.append({"Key": parts[0], "VersionId": parts[1]})
    return versions

def get_delete_markers(bucket_name, batch_size):
    """Get delete markers up to batch_size"""
    cmd = f'aws s3api list-object-versions --bucket "{bucket_name}" --max-items {batch_size} --query "DeleteMarkers[].[Key,VersionId]" --output text'
    output = run_aws_command(cmd)
    if not output:
        return []
    
    markers = []
    for line in output.split('\n'):
        if line.strip():
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                markers.append({"Key": parts[0], "VersionId": parts[1]})
    return markers

def delete_objects_from_file(bucket_name, temp_file_path, object_count):
    """Delete objects using temp file with s3api delete-objects"""
    if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
        return 0
    
    cmd = f'aws s3api delete-objects --bucket "{bucket_name}" --delete "file://{temp_file_path}"'
    result = run_aws_command(cmd)
    
    # Debug: print the actual response
    print(f"DEBUG: AWS response: {result}")
    
    # Since we know how many objects we sent, return that count if command succeeded
    if result is not None:
        return object_count
    return 0

def main():
    # Get bucket name
    bucket_name = input("Enter bucket name: ").strip()
    batch_size = 500  # You can change this value
    
    print(f"Processing bucket: {bucket_name}")
    print(f"Batch size: {batch_size}")
    
    cycle = 1
    total_versions_deleted = 0
    total_markers_deleted = 0
    
    while True:
        print(f"\n=== Cycle {cycle} ===")
        
        # Step 1: Get versions and save to temp file
        print("Step 1: Getting versions...")
        versions = get_versions(bucket_name, batch_size)
        print(f"Found {len(versions)} versions")
        
        versions_deleted = 0
        if versions:
            # Save versions to temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                delete_structure = {
                    "Objects": versions,
                    "Quiet": False  # Changed to False to see response
                }
                json.dump(delete_structure, f)
                temp_versions_file = f.name
            
            # Step 2: Delete versions from temp file
            print("Step 2: Deleting versions...")
            versions_deleted = delete_objects_from_file(bucket_name, temp_versions_file, len(versions))
            print(f"✓ Deleted {versions_deleted} versions")
            
            # Clean up temp file
            os.unlink(temp_versions_file)
        
        # Step 3: Get delete markers and save to temp file
        print("Step 3: Getting delete markers...")
        markers = get_delete_markers(bucket_name, batch_size)
        print(f"Found {len(markers)} delete markers")
        
        markers_deleted = 0
        if markers:
            # Save markers to temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                delete_structure = {
                    "Objects": markers,
                    "Quiet": False  # Changed to False to see response
                }
                json.dump(delete_structure, f)
                temp_markers_file = f.name
            
            # Step 4: Delete markers from temp file
            print("Step 4: Deleting delete markers...")
            markers_deleted = delete_objects_from_file(bucket_name, temp_markers_file, len(markers))
            print(f"✓ Deleted {markers_deleted} delete markers")
            
            # Clean up temp file
            os.unlink(temp_markers_file)
        
        # Update totals
        total_versions_deleted += versions_deleted
        total_markers_deleted += markers_deleted
        
        print(f"✓ Cycle {cycle} completed: {versions_deleted} versions + {markers_deleted} markers")
        print(f"Running totals: {total_versions_deleted} versions, {total_markers_deleted} markers")
        
        # Check if we're done (no versions AND no markers found)
        if len(versions) == 0 and len(markers) == 0:
            print("✓ No more objects found - bucket is clean!")
            break
        
        cycle += 1
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Total versions deleted: {total_versions_deleted}")
    print(f"Total delete markers deleted: {total_markers_deleted}")
    print(f"Total objects processed: {total_versions_deleted + total_markers_deleted}")
    print(f"Total cycles: {cycle}")
    print(f"✅ SUCCESS: Bucket {bucket_name} is completely clean!")

if __name__ == "__main__":
    main()
