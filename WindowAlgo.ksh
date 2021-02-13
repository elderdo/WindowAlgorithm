JRE=/opt/java1.2/jre
CLASSPATH=$JRE/lib:WindowAlgo.jar:classes12.zip:log4j-1.2.3.jar;log4j.properties;.
$JRE/bin/java SparePart WindowAlgo.ini > sparePart.log
$JRE/bin/java OnOrder WindowAlgo.ini > onOrder.log
$JRE/bin/java OnHandInvs WindowAlgo.ini > onOrder.log
$JRE/bin/java InRepair WindowAlgo.ini > inRepair.log

