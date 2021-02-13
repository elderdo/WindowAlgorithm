echo on
SET OLDP=%PATH%
SET OLDCP=%CLASSPATH%
SET JRE=\\dev-sc1-50\h\TmapiFiles\Jre\1.3.1\
SET PATH=%JRE%bin;.
SET CLASSPATH=%JRE%lib;WindowAlgo.jar;classes12.zip;.
java.exe SparePart > output.log
SET PATH=%OLDP%
SET CLASSPATH=%OLDCP%
