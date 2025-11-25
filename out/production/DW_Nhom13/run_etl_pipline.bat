@echo off
TITLE ETL JOB MASTER (PARTIAL RUN) - D:\ETL_Jobs\
COLOR 0A

:: ========================================================
:: PHAN 1: CAU HINH & KIEM TRA FILE
:: ========================================================

cd /d "D:\ETL_Jobs\"

:: !!! QUAN TRONG: Chi dien ten 2 file minh co vao day thoi !!!
SET "ETL_FILES=CrawlData.jar Staging.jar"

echo [SYSTEM] Dang kiem tra cac module hien co...
echo.

SET /A MISSING_COUNT=0

FOR %%F IN (%ETL_FILES%) DO (
    IF EXIST "%%F" (
        echo [OK] Found module: %%F
    ) ELSE (
        COLOR 4F
        echo [MISSING] !!! KHONG TIM THAY: %%F
        SET /A MISSING_COUNT+=1
    )
)

IF %MISSING_COUNT% GTR 0 (
    echo [ERROR] Thieu file core (Crawl hoac Staging). Khong the chay!
    pause
    exit
)

:: ========================================================
:: PHAN 2: THUC THI QUY TRINH (Chay cai co, bo cai thieu)
:: ========================================================

cls
echo.
echo [SUCCESS] Cac module cot loi da san sang.
echo Bat dau chay quy trinh ETL (Phien ban demo Phase 1)...
echo Thoi gian bat dau: %date% %time%
echo ========================================================

:: 1. Chay Crawl Data (CO FILE -> CHAY)
echo.
echo [1/4] >>> DANG CHAY CRAWL DATA...
java -jar "CrawlData.jar"
IF %ERRORLEVEL% NEQ 0 GOTO :ErrorHandle

:: 2. Chay Staging (CO FILE -> CHAY)
echo.
echo [2/4] >>> DANG CHAY STAGING...
java -jar "Staging.jar"
IF %ERRORLEVEL% NEQ 0 GOTO :ErrorHandle

:: 3. Chay Warehouse (THIEU FILE -> THONG BAO PENDING)
echo.
echo [3/4] ... KET NOI WAREHOUSE ...
echo [INFO] Module Warehouse dang duoc phat trien (Pending).
echo [INFO] Bo qua buoc nay, du lieu dang dung o Staging.
:: java -jar "Warehouse.jar" <-- Da comment lai de khong chay

:: 4. Chay Data Mart (THIEU FILE -> THONG BAO PENDING)
echo.
echo [4/4] ... KET NOI DATA MART ...
echo [INFO] Module DataMart dang duoc phat trien (Pending).
:: java -jar "DataMart.jar" <-- Da comment lai de khong chay

echo.
echo ========================================================
echo [DONE] QUY TRINH HOAN TAT (DUNG TAI STAGING AREA)!
echo Du lieu da duoc lam sach va luu tai Staging DB.
echo ========================================================
pause
exit

:ErrorHandle
COLOR 4F
echo.
echo [RUNTIME ERROR] Loi xay ra o module Crawl hoac Staging!
pause