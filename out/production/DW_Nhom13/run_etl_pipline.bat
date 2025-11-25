@echo off
TITLE ETL DEBUG MODE
cd /d "%~dp0"

echo [DEBUG] Bat dau script...
echo [DEBUG] Thu muc hien tai: %CD%

:: --- CAU HINH ---
SET "ETL_FILES=CrawlData.jar Staging.jar"
SET /A MISSING_COUNT=0

echo ------------------------------------------
echo [DEBUG] Dang kiem tra file...

FOR %%F IN (%ETL_FILES%) DO (
    IF EXIST "%%F" (
        echo [OK] Thay file: %%F
    ) ELSE (
        echo [MISSING] Khong thay file: %%F
        SET /A MISSING_COUNT+=1
    )
)

echo ------------------------------------------
echo [DEBUG] Kiem tra xong. So file thieu: %MISSING_COUNT%
:: Dung lai de xem ket qua check file

IF %MISSING_COUNT% GTR 0 (
    echo [ERROR] Thieu file roi!
    echo Vui long kiem tra lai folder.
    exit
)

echo [DEBUG] Du file -> Chay tiep...

:: --- PHAN 2 ---
echo.
echo [1/4] CrawlData...
java -jar "CrawlData.jar"
IF %ERRORLEVEL% NEQ 0 (
    echo [LOI] CrawlData chet!
    goto :ErrorHandle
)

echo.
echo [2/4] Staging...
java -jar "Staging.jar"
IF %ERRORLEVEL% NEQ 0 (
    echo [LOI] Staging chet!
    goto :ErrorHandle
)

echo.
echo [3/4] Warehouse (Pending)...
echo [INFO] Bo qua Warehouse.

echo.
echo [4/4] DataMart (Pending)...
echo [INFO] Bo qua DataMart.

echo.
echo ========================
echo XONG!
echo ========================
exit

:ErrorHandle
echo.
echo [CRITICAL ERROR] Chuong trinh bi loi!