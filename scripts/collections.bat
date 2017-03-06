@echo off
set loop=2
set data_folder=d:\kolekce\DERI\
set exec=..\x64\release\rtree_gpu.exe
set tmpDir=d:\tmp
setlocal enabledelayedexpansion
set /A totalCalls=6 * 5 * 4 * 1;
set currentCall=1
if not exist {%tmpDir%} (mkdir %tmpDir%)


call:run "POKER" "%data_folder%POKER\Export.txt" "%data_folder%POKER\Queries.txt"
call:run "TIGER" "%data_folder%TIGER\Export.txt" "%data_folder%TIGER\Queries.txt"
ENDLOCAL
echo.&pause&goto:eof
                                       
:run
FOR /d %%T IN (128 256 384 512 768 1024) DO (
  FOR /d %%C IN (16 32 64 128 256) DO (
    FOR /d %%B IN (2048 4096 8192 16384) DO (
      set resultFile=%CD%\%~1_%%B_chunks_%%C_tpb_%%T.csv
      set dbFile=%tmpDir%\%~1_%%B
      set create=1
      if exist !dbFile!.dat set create=0
      if exist !resultFile! del !resultFile!
      ::echo --create-db !create! -bs %%B -c %~2 -q %~3 -db !dbFile! -r !resultFile! --gpu-chunks %%C --gpu-tpb %%T
      call:info %~1 %~2 %~3 %%B 1 %loop% !dbFile!
      %exec% --create-db !create! -bs %%B -c %~2 -q %~3 -db !dbFile! -r !resultFile! --gpu-chunks %%C --gpu-tpb %%T
      FOR /l %%I IN (2,1,%loop%) DO    (
        call:info %~1 %~2 %~3 %%B %%I %loop%
        %exec% --create-db 0 -bs %%B -c %~2 -q %~3 -db !dbFile! -r !resultFile! --gpu-chunks %%C --gpu-tpb %%T
      )
    )
  )
)
goto:eof 

:info
cls
echo Testuji: %~1
echo Collection: %~2
echo Queries: %~3
echo Buffer size: %~4
echo Test %~5 of %~6
echo Celkem test %currentCall% z %totalCalls%
set /A currentCall+=1
echo 
goto:eof 
