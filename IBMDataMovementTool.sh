#!/bin/sh
########################################################################  
###  Copyright(r) IBM Corporation
###
###  Vikram Khatri (vikram.khatri@us.ibm.com)
###

### ------------------------------------------------------------------- 
### IBM Data Movement Tool for Data Movement from source to DB2
### ------------------------------------------------------------------- 
########################################################################  


MINVERSION=1.5
MINVERSION=`echo $MINVERSION | sed -e 's;\.;0;g'`
 
java -version > tmp.ver 2>&1

VERSION=`cat tmp.ver | grep "java version" | awk '{ print substr($3, 2, length($3)-2); }'`
rm tmp.ver
VERSION=`echo $VERSION | awk '{ print substr($1, 1, 3); }' | sed -e 's;\.;0;g'`
if [ $VERSION ] 
then
    if [ $VERSION -lt $MINVERSION ] 
    then
 	echo Your Version of Java is not 1.5.0
        exit
    fi
fi
java -Xmx990m -jar IBMDataMovementTool.jar $1
