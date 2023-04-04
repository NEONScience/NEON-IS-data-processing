#!/bin/bash

echo "Output path = $OUT_PATH"
echo "Start month = $START_MONTH"
# If END_MONTH unset, set to previous month
if [ -z $END_MONTH ] 
then 
  END_MONTH=$(date --date="$(date +%Y-%m-01) -1 month" -u +%Y-%m);
  echo "Input END_MONTH is unset. Setting to previous month."
fi
echo "End month = $END_MONTH"

# Create monthly folder structure 
echo "Creating monthly folder structure import trigger folder structure for $site"
start_date_secs=$(date -u +%s -d "$START_MONTH-01")
end_date_secs=$(date -u +%s -d "$END_MONTH-01")
d=$start_date_secs
while [ $d -le $end_date_secs ]
do
  # Creat this month's path 
  month_path=$(date -u +%Y/%m -d @$d);
  
  # Write empty file
  mkdir -p $OUT_PATH/$month_path;
  echo "Writing $OUT_PATH/$month_path/.empty"
  touch $OUT_PATH/$month_path/.empty;

  # Get next month
  d=$(date --date="$(date -u -d $month_path/01) +1 month" -u +%s);
done
