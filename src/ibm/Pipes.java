/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

/*###################################################################################################
#
#  Author:      Vikram Khatri (vikram.khatri@us.ibm.com)
#
# (C) COPYRIGHT International Business Machines Corp. 2006-2007
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

public class Pipes
{
	static
	{
		String arch = System.getProperty( "sun.arch.data.model" );

		String lib = (arch.equals("64") ? "Pipe64" : "Pipe");
		IBMExtractUtilities.log("Loaded Library " + lib);
		System.loadLibrary(lib);
	}

	public static final native int CreateNamedPipe(String pipeName,
			int ppenMode, int pipeMode, int maxInstances,
			int outBufferSize, int inBufferSize, int defaultTimeOut,
			int securityAttributes);

	public static final native boolean ConnectNamedPipe(int namedPipeHandle, int overlapped);
	public static final native int GetLastError();
	public static final native boolean CloseHandle(int bbject);
	public static final native byte[] ReadFile(int file, int numberOfBytesToRead);
	public static final native int WriteFile(int file, byte[] buffer, int numberOfBytesToWrite);
	public static final native boolean FlushFileBuffers(int file);
	public static final native boolean DisconnectNamedPipe(int namedPipeHandle);
	public static final native int CreateFile(String fileName,
			int desiredAccess, int shareMode, int securityAttributes,
			int creationDisposition, int flagsAndAttributes,
			int templateFile);

	public static final native boolean WaitNamedPipe(String namedPipeName, int timeOut);
	public static final native String FormatMessage(int errorCode);
	public static final native void Print(String message);
}
