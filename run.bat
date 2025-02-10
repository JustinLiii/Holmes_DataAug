@echo off
SET batch_start=0
SET batch_end=5

ECHO Begin batch processing
python make_batch_file.py --start %batch_start% --end %batch_end%
ECHO Batch file created

ECHO Uploading batch file
python upload_batch_file.py --start %batch_start% --end %batch_end%
ECHO Batch file uploaded

ECHO Starting batch task
python start_batch.py --start %batch_start% --end %batch_end%
ECHO Batch task started

ECHO Downloading batch task
python download_batch.py
