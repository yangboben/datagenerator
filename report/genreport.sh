BASEDIR=$(cd `dirname $0`; cd ..;pwd)
cd $BASEDIR
CLASSPATH=$(mvn dependency:build-classpath|grep m2)
java -cp $CLASSPATH:$BASEDIR/target/streamsql-test-1.0.jar org.apache.spark.transwarp.StreamReportGen $@
