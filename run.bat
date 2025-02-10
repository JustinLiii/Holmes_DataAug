@echo off
if "%1"=="" (
    echo Usage: run.bat [batch_start] [batch_end]
    exit /b
)

if "%2"=="" (
    echo Usage: run.bat [batch_start] [batch_end]
    exit /b
)

if not "%3"=="" (
    echo Usage: run.bat [batch_start] [batch_end]
    exit /b
)

SET batch_start=%1
SET batch_end=%2

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
python download_batch.py --start %batch_start% --end %batch_end%
