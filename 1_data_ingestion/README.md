To run these scripts periodically, schedule them using cron.

Example cron job to run ingestion every day at midnight:-
0 0 * * * /usr/bin/python3 /ingest_csv_data.py
0 0 * * * /usr/bin/python3 /ingest_api_data.py