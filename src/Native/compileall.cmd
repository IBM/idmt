copy "C:\Users\Vikram\Documents\Visual Studio 2008\Projects\Pipe\Pipe\Pipe.c"
copy "C:\Users\Vikram\Documents\Visual Studio 2008\Projects\Pipe\Pipe\ibm_Pipes.h"

call "C:\Program Files (x86)\Microsoft Visual Studio 9.0\VC\vcvarsall.bat" x86

call compile.cmd

call "C:\Program Files (x86)\Microsoft Visual Studio 9.0\VC\vcvarsall.bat" x86_amd64

call compile.cmd 64

del Pipe.exp
del Pipe.lib
del Pipe.obj
del Pipe64.lib
del Pipe64.exp



