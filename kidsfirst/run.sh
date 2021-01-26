#! /bin/bash

# Create a PFB file containing a Kids First study's data
# Set the KF_DATASERVICE_DB_URL env var to the connection URL for the DB
#
# Usage: ./kidsfirst/run.sh <KF study id>


set -eo pipefail

study_id="$1"
script_dir=$(dirname $0)
output_dir="$script_dir/$study_id"

if [[ -z $study_id ]];
then
    echo "You must provide the study id for the script to continue. Aborting!"
    exit 1
fi

echo "🏭 Start Kids First PFB file export. Stream $study_id data into PFB file"

# Update SQL templates with study id
for fp in $script_dir/sql/*.sql;
do
    echo "Creating SQL file for $fp"
    sed "s/REPLACE_STUDY_ID/$study_id/g" "$fp" > "$fp-temp"
done

# Run PFB export for each SQL file
for fp in $script_dir/sql/*.sql-temp;
do
    echo "Processing file $fp"
    fn="${fp##*/}"
    entity="${fn%.*}"
    pfbe db_export "$KF_DATASERVICE_DB_URL" \
                    -t "$entity" \
                    -s "$fp" \
                    -o "$output_dir" \
                    --no_overwrite
    
    log_file="$entity-pfb_export.log"
    mv $output_dir/logs/pfb_export.log $output_dir/logs/$log_file
done

# Remove temporary SQL files
rm $script_dir/sql/*.sql-temp

echo "✅ Finished running Kids First PFB file export for study $study_id"