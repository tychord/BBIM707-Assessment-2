
#!/bin/bash

# Part B - Task 1 - Step 7: Automated HDFS ingestion and recovery

set -euo pipefail

# Define local and HDFS paths
SOURCE="$HOME/BBIM707_Task3/olist_integrated_clean.csv"
HDFS_DIR="/user/hp/olist_data"
DEST="$HDFS_DIR/olist_integrated_clean.csv"
TEMP="$HDFS_DIR/olist_integrated_clean.csv.uploading"
BACKUP="$HDFS_DIR/olist_integrated_clean.csv.backup"
MAX_ATTEMPTS=3

# Check source file availability
if [ ! -f "$SOURCE" ]; then
    echo "ERROR: Source dataset not found."
    exit 1
fi

# Calculate original dataset checksum
LOCAL_HASH=$(sha256sum "$SOURCE" | awk '{print $1}')

echo "Starting automated HDFS ingestion..."
echo "Source: $SOURCE"
echo "Destination: $DEST"

# Ensure HDFS directory exists
hdfs dfs -mkdir -p "$HDFS_DIR"

# Retry ingestion up to three times
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do

    echo "Ingestion attempt: $attempt"

    # Remove incomplete temporary uploads
    hdfs dfs -rm -f "$TEMP" >/dev/null 2>&1 || true

    # Upload to temporary HDFS location
    if ! hdfs dfs -put "$SOURCE" "$TEMP"; then
        echo "Upload failed."
        sleep 2
        continue
    fi

    # Verify uploaded file integrity
    if ! HDFS_HASH=$(hdfs dfs -cat "$TEMP" | sha256sum | awk '{print $1}'); then
        echo "Checksum calculation failed."
        sleep 2
        continue
    fi

    if [ "$LOCAL_HASH" != "$HDFS_HASH" ]; then
        echo "ERROR: Checksum mismatch."
        sleep 2
        continue
    fi

    echo "SHA-256 checksum verified."

    # Preserve the existing destination before replacement
    if hdfs dfs -test -e "$DEST"; then
        if ! hdfs dfs -test -e "$BACKUP"; then
            if ! hdfs dfs -mv "$DEST" "$BACKUP"; then
                echo "ERROR: Could not back up existing dataset."
                exit 1
            fi
        else
            echo "ERROR: Existing backup found. Review before proceeding."
            exit 1
        fi
    fi

    # Promote the verified temporary file
    if hdfs dfs -mv "$TEMP" "$DEST"; then
        echo "SUCCESS: Dataset uploaded and verified."
        echo "SHA-256: $HDFS_HASH"
        echo "Destination: $DEST"

        # Remove backup after successful replacement
        if hdfs dfs -test -e "$BACKUP"; then
            echo "Backup retained at: $BACKUP"
        fi

        exit 0
    fi

    echo "Promotion failed. Attempting recovery."

    # Restore previous dataset if available
    if hdfs dfs -test -e "$BACKUP"; then
        if ! hdfs dfs -test -e "$DEST"; then
            hdfs dfs -mv "$BACKUP" "$DEST"
        fi
    fi

    sleep 2
done

echo "ERROR: Ingestion failed after $MAX_ATTEMPTS attempts."
exit 1

