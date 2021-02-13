echo on
SET OLDP=%PATH%
SET OLDCP=%CLASSPATH%
SET JRE=C:\PROGRA~1\j2sdk_nb\J2SDK1~1.2\jre\
SET PATH=%JRE%bin;.
SET CLASSPATH=%JRE%lib;WindowAlgo.jar;classes12.zip;log4j-1.2.3.jar;log4j.properties;.
rem java.exe SparePart WindowAlgo.ini > sparePart.log
rem java.exe OnOrder WindowAlgo.ini > onOrder.log
rem java.exe OnHandInvs WindowAlgo.ini > onHandInvs.log
java.exe InRepair WindowAlgo.ini > inRepair.log
echo OnOrder rc=%errorlevel% 
SET PATH=%OLDP%
SET CLASSPATH=%OLDCP%
