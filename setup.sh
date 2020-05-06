docker-compose up -d
sleep 15
docker-compose exec kafka kafka-topics --create --topic job --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec kafka kafka-topics --create --topic db --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker-compose exec kafka kafka-topics --create --topic error --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181