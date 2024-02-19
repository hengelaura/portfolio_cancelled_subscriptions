#!/bin/bash
# This script will call the python cleaning script and check the error and change logs
# If there is an update to the database, the resulting csv and database files would be moved from the dev folder to the prod folder

CURTIME=$(date +%s)
C:/Users/henge/AppData/Local/Programs/Python/Python312/python.exe cancelled_subs.py
CHANGETIME=$(stat change.log -c %Y)
ERRTIME=$(stat error.log -c %Y)

changediff=$(expr $CURTIME - $CHANGETIME)
errdiff=$(expr $CURTIME - $ERRTIME)


if [ $changediff -lt 60 ]
then
   last_line=$( tail -n 1 change.log )
   if [[ "$last_line" == *"No new data to upload"* ]]
   then
      update_flag=0
   elif [[ "$last_line" == *"new rows of data"* ]]
   then
      update_flag=1
   fi
   change_flag=1
else
   change_flag=0
fi

if [ $errdiff -lt 60 ]
then
    err_flag=1
else
    err_flag=0
fi

if [ $err_flag -eq 1 ]
then
   echo "Errors present in process. Please review error.log for messages"
elif [ $change_flag -eq 1 ]
then
   if [ $update_flag -eq 0 ]
   then
      echo "No new data to upload. Process complete."
   else
      echo "New data uploaded. Updating files in prod folder."
      $(cp -f ../dev/final_table.csv ../prod/.)
      $(cp -f ../dev/final_table.db ../prod/.)
   fi
fi


