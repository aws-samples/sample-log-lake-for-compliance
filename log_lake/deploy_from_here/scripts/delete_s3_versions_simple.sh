#!/bin/bash
read -p "Enter bucket(s) to delete (comma-separated): " BUCKET_INPUT

# Split the input by commas and process each bucket
IFS=',' read -ra BUCKET_ARRAY <<< "$BUCKET_INPUT"

for BUCKET_NAME in "${BUCKET_ARRAY[@]}"; do
    # Trim whitespace
    BUCKET_NAME=$(echo "$BUCKET_NAME" | xargs)
    echo "Processing bucket: $BUCKET_NAME"
    
    # Check if bucket has any versions
    VERSIONS=$(aws s3api list-object-versions --bucket "${BUCKET_NAME}" --query 'Versions[].Key' --output text)
    if [ -z "$VERSIONS" ] || [ "$VERSIONS" = "None" ]; then
        echo "Bucket ${BUCKET_NAME} has no object versions to delete"
    else
        # First, delete all object versions
        aws s3api delete-objects --bucket "${BUCKET_NAME}" --delete "$(aws s3api list-object-versions --bucket "${BUCKET_NAME}" --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --max-items 1000)"
        echo "Deleted object versions from ${BUCKET_NAME}"
    fi

    # Check if bucket has any delete markers
    DELETE_MARKERS=$(aws s3api list-object-versions --bucket "${BUCKET_NAME}" --query 'DeleteMarkers[].Key' --output text)
    if [ -z "$DELETE_MARKERS" ] || [ "$DELETE_MARKERS" = "None" ]; then
        echo "Bucket ${BUCKET_NAME} has no delete markers to remove"
    else
        # Then delete all delete markers
        aws s3api delete-objects --bucket "${BUCKET_NAME}" --delete "$(aws s3api list-object-versions --bucket "${BUCKET_NAME}" --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --max-items 1000)"
        echo "Deleted delete markers from ${BUCKET_NAME}"
    fi
    
    echo "Finished processing bucket: ${BUCKET_NAME}"
    echo "..."
done

echo "All buckets processed...done."
