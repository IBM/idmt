/*###################################################################################################
#
#  Author:      Vikram Khatri (vikram.khatri@us.ibm.com)
#
# (C) COPYRIGHT International Business Machines Corp. 2006-2010
#
# All Rights Reserved
#
# This software ("Software") is owned by International Business Machines Corporation or one of its 
# subsidiaries ("IBM") and is copyrighted and licensed, not sold. This Software is not part of any 
# standard IBM product. You may use, copy, modify, and distribute this Software in any form without 
# payment to IBM, for the purpose of assisting you in the development of your applications. This 
# Software is provided to you on an "AS IS" basis, without warranty of any kind. IBM HEREBY EXPRESSLY 
# DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do not allow 
# for the exclusion or limitation of implied warranties, so the above limitations or exclusions may not 
# apply to you. IBM shall not be liable for any damages you suffer as a result of using, copying, 
# modifying or distributing this Software, even if IBM has been advised of the possibility 
# of such damages. 
#
# US Government Users Restricted Rights - Use, duplication or disclosure restricted 
# by GSA ADP Schedule Contract with IBM Corp.
#
###################################################################################################*/


#include <windows.h> 
#include <strsafe.h>
#include <jni.h>
#include "ibm_Pipes.h"

#define	DEBUG	0

JNIEXPORT jint JNICALL Java_ibm_Pipes_CreateNamedPipe
(
	 JNIEnv *env, 
	 jclass className, 
	 jstring sPipeName, 
	 jint dwOpenMode, 
	 jint dwPipeMode, 
	 jint nMaxInstances, 
	 jint nOutBufferSize, 
	 jint nInBufferSize, 
	 jint nDefaultTimeOut, 
	 jint lpSecurityAttributes
 )
{
	HANDLE pipeHandler;
	LPCSTR pipeName;
	pipeName = (*env)->GetStringUTFChars(env, sPipeName, NULL);
	if (pipeName == NULL)
		return -1;

	if (DEBUG)
	{
		printf("Native: Pipe Name %s\n", pipeName);
		printf("Native: dwOpenMode %d\n", dwOpenMode);
		printf("Native: dwPipeMode %d\n", dwPipeMode);
		printf("Native: nMaxInstances %d\n", nMaxInstances);
		printf("Native: nOutBufferSize %d\n", nOutBufferSize);
		printf("Native: nInBufferSize %d\n", nInBufferSize);
		printf("Native: nDefaultTimeOut %d\n", nDefaultTimeOut);
	}

	pipeHandler = CreateNamedPipe((LPCSTR)pipeName, dwOpenMode, dwPipeMode, nMaxInstances, nOutBufferSize, nInBufferSize, 
		nDefaultTimeOut, (LPSECURITY_ATTRIBUTES) lpSecurityAttributes);  

	(*env)->ReleaseStringUTFChars(env, sPipeName, pipeName);
	return (jint) pipeHandler;
}

JNIEXPORT jboolean JNICALL Java_ibm_Pipes_ConnectNamedPipe
(
	 JNIEnv *env, 
	 jclass className, 
	 jint hNamedPipe, 
	 jint lpOverlapped
 )
{
	BOOL fConnected;
	HANDLE pipeHandler = (HANDLE) hNamedPipe;
	fConnected = ConnectNamedPipe(pipeHandler, (LPOVERLAPPED) lpOverlapped);
	return fConnected;
}

JNIEXPORT jint JNICALL Java_ibm_Pipes_GetLastError 
(
	 JNIEnv *env, 
	 jclass className
 )
{
	DWORD errorNumber = GetLastError();
	return (jint) errorNumber;
}

JNIEXPORT jboolean JNICALL Java_ibm_Pipes_CloseHandle
(
	 JNIEnv *env, 
	 jclass className, 
	 jint hNamedPipe
 )
{
	BOOL result;
    HANDLE pipeHandler = (HANDLE) hNamedPipe;
	result = CloseHandle(pipeHandler);
	return result;
}

JNIEXPORT jbyteArray JNICALL Java_ibm_Pipes_ReadFile
(
	 JNIEnv *env, 
	 jclass className, 
	 jint hNamedPipe, 
	 jint nNumberOfBytesToRead
 )
{
	int bytesRead = 0;
	BOOL result;
    HANDLE pipeHandler = (HANDLE) hNamedPipe;
	LPVOID buffer;
	jbyteArray lpBuffer;

	buffer = (LPVOID)LocalAlloc(LMEM_ZEROINIT, nNumberOfBytesToRead);

	if (DEBUG)
	{
		printf("Native: Before ReadFile pipeHandler %d nNumberOfBytesToRead %d\n", pipeHandler, nNumberOfBytesToRead);
	}
	result = ReadFile(pipeHandler, (LPVOID) buffer, (DWORD) nNumberOfBytesToRead, &bytesRead, (LPOVERLAPPED) 0);
	if (result)
	{
		lpBuffer = (*env)->NewByteArray(env, (jsize) bytesRead);		
		(*env)->SetByteArrayRegion(env, lpBuffer, 0, (jsize) bytesRead, (jbyte *) buffer);
	} else
		bytesRead = 0;

	LocalFree(buffer);

	if (DEBUG)
	{
		printf("Native: After ReadFile BytesRead %d\n", bytesRead);
	}
	return lpBuffer;
}

JNIEXPORT jint JNICALL Java_ibm_Pipes_WriteFile
(
	 JNIEnv *env, 
	 jclass className, 
	 jint hNamedPipe, 
	 jbyteArray lpBuffer, 
	 jint nNumberOfBytesToWrite
 )
{
	int bytesWritten = 0;
	BOOL result;
    HANDLE pipeHandler = (HANDLE) hNamedPipe;
	LPVOID buffer;

	buffer = (LPVOID)LocalAlloc(LMEM_ZEROINIT, nNumberOfBytesToWrite);

	(*env)->GetByteArrayRegion(env, lpBuffer, 0, nNumberOfBytesToWrite, buffer);
	result = WriteFile(pipeHandler, buffer, (DWORD) nNumberOfBytesToWrite, (LPDWORD) &bytesWritten, (LPOVERLAPPED) 0);
	LocalFree(buffer);

	if (DEBUG)
	{
		printf("Native: After WriteFile BytesReadWritten %d\n", bytesWritten);
	}

	if (!result)
	{
		if (GetLastError() != ERROR_IO_PENDING)
			result = 0;
		else
			result = 1;
	}
	if (!result)
	{
		bytesWritten = -1;
	}
	return bytesWritten;
}

JNIEXPORT jboolean JNICALL Java_ibm_Pipes_FlushFileBuffers
(
	 JNIEnv *env, 
	 jclass className, 
	 jint hNamedPipe
 )
{
	BOOL result;
    HANDLE pipeHandler = (HANDLE) hNamedPipe;
	result = FlushFileBuffers(pipeHandler); 
	return result;
}

JNIEXPORT jboolean JNICALL Java_ibm_Pipes_DisconnectNamedPipe
(
	 JNIEnv *env, 
	 jclass className, 
	 jint hNamedPipe
 )
{
	BOOL result;
    HANDLE pipeHandler = (HANDLE) hNamedPipe;
	result = DisconnectNamedPipe(pipeHandler);
	return result;
}

JNIEXPORT jint JNICALL Java_ibm_Pipes_CreateFile
(
	 JNIEnv *env, 
	 jclass className, 
	 jstring lpFileName, 
	 jint dwDesiredAccess, 
	 jint dwShareMode, 
	 jint lpSecurityAttributes, 
	 jint dwCreationDisposition, 
	 jint dwFlagsAndAttributes, 
	 jint hTemplateFile
 )
{
    HANDLE pipeHandler;
	const jbyte *fileName;
	fileName = (*env)->GetStringUTFChars(env, lpFileName, NULL);
	if (fileName == NULL)
		return -1;
	pipeHandler = CreateFile((LPCSTR) fileName, (DWORD) dwDesiredAccess, (DWORD) dwShareMode, 
		(LPSECURITY_ATTRIBUTES) lpSecurityAttributes, (DWORD) dwCreationDisposition, (DWORD) dwFlagsAndAttributes, (HANDLE) hTemplateFile);
	return (jint) pipeHandler;
}

JNIEXPORT jboolean JNICALL Java_ibm_Pipes_WaitNamedPipe
(
	 JNIEnv *env, 
	 jclass className, 
	 jstring lpNamedPipeName, 
	 jint nTimeOut
 )
{
	BOOL result;
	const jbyte *pipeName;
	pipeName = (*env)->GetStringUTFChars(env, lpNamedPipeName, NULL);
	if (pipeName == NULL)
		return 0;
	result = WaitNamedPipe((LPCSTR) pipeName, (DWORD) nTimeOut);
	return result;
}

JNIEXPORT jstring JNICALL Java_ibm_Pipes_FormatMessage
(
	JNIEnv *env, 
	jclass className, 
	jint errorCode
)
{
	LPVOID lpMsgBuf;
    LPVOID lpDisplayBuf;
	DWORD dw = (DWORD) errorCode;

	FormatMessage(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | 
        FORMAT_MESSAGE_FROM_SYSTEM |
        FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        dw,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPTSTR) &lpMsgBuf,
        0, NULL );

	lpDisplayBuf = (LPVOID)LocalAlloc(LMEM_ZEROINIT, 
        (lstrlen((LPCTSTR)lpMsgBuf) + 40) * sizeof(TCHAR)); 
    StringCchPrintf((LPTSTR)lpDisplayBuf, LocalSize(lpDisplayBuf) / sizeof(TCHAR),
        TEXT("Failed with error %d: %s"), dw, lpMsgBuf); 
	return (jstring) (*env)->NewStringUTF(env, lpDisplayBuf);
}

JNIEXPORT void JNICALL Java_ibm_Pipes_Print(JNIEnv *env, jclass className, jstring lpMsgBuf)
{	
	const jbyte *str;
	str = (*env)->GetStringUTFChars(env, lpMsgBuf, NULL);
	if (str == NULL)
		return;
	printf("Native: %s\n", str);
	(*env)->ReleaseStringUTFChars(env, lpMsgBuf, str);
	return;
}
