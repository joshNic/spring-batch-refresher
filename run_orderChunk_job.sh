CURRENT_DATE=`date '+%Y/%m/%d'`
LESSON=$(basename $PWD)
mvn clean package -Dmaven.test.skip=true;
java -jar -Dspring.batch.job.names=orderJob ./target/springBatch-0.0.1-SNAPSHOT.jar "run.date(date)=$CURRENT_DATE" "lesson=$LESSON";
read;