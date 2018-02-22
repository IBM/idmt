@echo OFF
SET BIT=
if "%1" == "64" set BIT=64

ECHO COMPILING %BIT%

cl -I"C:\Program Files (x86)\Java\Java50\include" -I"C:\Program Files (x86)\Java\Java50\include\win32" -LD Pipe.c -FePipe%BIT%.dll
copy Pipe%BIT%.dll ..\..\

