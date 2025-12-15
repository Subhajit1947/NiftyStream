settingpermission-for-jarfiles:
	docker-compose exec --user root spark-master bash -c "
    mkdir -p /nonexistent/.ivy2/cache
    chmod 777 /nonexistent/.ivy2
    chmod 777 /nonexistent/.ivy2/cache
"

run-spark:
	docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/apps/spark_streaming_test.py

run-spark-postgres:
    docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.postgresql:postgresql:42.7.3 /opt/apps/spark_streaming_test.py