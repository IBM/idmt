# idmt
IBM Data Movement Tool
======================

History
=======

IBM Data Movement Tool started with 300 lines of Java program to just unload data using a JDBC connection to an Oracle database. Back in 2004, when I was doing a migration for a customer using IBM Migration Tool Kit (MTK), the customer estimated that it will take 7 days to migrate a table having 3 million rows and they laughed. Their comment was "Come back when you have a better tool". This was a wake-up call to me that we have to have a much better faster and better data migration tool. I approached the MTK team and their mandate was different by then and they did not had any bandwidth to make any modifications.

I was not a professional developer but I knew little bit of Java programming same way an application developer knows little bit of DBA work.

I started working on the tool and starting using it with every customer engagement. Getting real customer feedback and making changes on the spot was the way tool was managed. The tool was purely customer driven requirements. The tool got better with each engagement and I kept on adding support for different source databases like Oracle, SQL Server, Access, Terradata, Netezza, PostgreSQL, MySQL and many others from time to time.

The Java code is not very well organized and if a professional Java developer sees this code, they will tear their hairs and will comment "What a crapy code?". But this is what it is. It works and works very fast.

When First Published
====================

I used this tool only for myself for almost 6 years without anyone knowing about it. Then, people from IBM encouraged me to add a GUI so that it is usable by anyone and that was the hard part.

I still use the tool which is command line. The tool was first published without the source code in 2010. The link for the poorly written documentation is at : https://www.ibm.com/developerworks/data/library/techarticle/dm-0906datamovement/

Usage of the tool
=================

This tool has not been updated in last 6 years but I still get few downloads report every day from IBM download site. It is amazing that people are still using the tool.

Other Tools
===========

Then, I moved on and I handed over this tool to another team and they built IBM Database Conversion Workbench which was like a plugin to Eclipse but they did not bring all functionality and all source databases from the base code to the new tool.

DCW - database conversion workbench is still supported and people should use that tool if that serves your purpose.

Later on IBM Harmony profiler tool was just a command line the way I wanted that to be.

Why open source?
================

I was the only contributer to the tool and it has some of the very powerful features which are not in the tool. It is still the workhorse and can do a much better job of high speed migration in parallel and achieve 1 TB / hour migration speed.

I approached IBM and took the permission to open source it so that the code remains in public domain if down the line, this tool is still used and some modifications are required.

The organization of the code is complex and I will take some time in future to document it so that if a brave soul decides to use or modify it so that they can get started easily.

Usage
=====

Please refer to the IBM article for the usage of the tool. https://www.ibm.com/developerworks/data/library/techarticle/dm-0906datamovement/

Download
========

The complied version of the tool (Jar) can be downloaded from this link:

https://www-01.ibm.com/marketing/iwm/iwm/web/preLogin.do?lang=en_US&source=idmt

Organization
============

The main folder under source is ibm which contains the heart of the tool - which is command line.

The com and jsyntaxpane folders are for GUI part and mostly open source. I have not touched this code and just used for the GUI portion.

The main tool is GenerateExtract.java and IBMExtractConfig.java for getting inout and generating the command files. The IBMExtractGUI2.java is the main entry point for the GUI.

https://www-01.ibm.com/marketing/iwm/iwm/web/preLogin.do?lang=en_US&source=idmt

Further Work
============

I have no plan to enhance or modify the tool unless I am doing some very large migrations myself.
