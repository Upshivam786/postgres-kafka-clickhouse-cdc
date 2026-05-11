.PHONY: start stop status register-connector check-topics

start:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 30
	@docker compose ps

stop:
	docker compose down

status:
	@echo "=== Connector Status ==="
	@curl -s http://localhost:8083/connectors/postgres-cdc-connector/status | python3 -m json.tool
	@echo "\n=== Kafka Topics ==="
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep pgcdc

register-connector:
	@curl -s -X POST \
	  -H "Content-Type: application/json" \
	  --data @debezium/connector.json \
	  http://localhost:8083/connectors | python3 -m json.tool

check-topics:
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

check-lag:
	@docker exec kafka kafka-consumer-groups \
	  --bootstrap-server localhost:9092 \
	  --all-groups --describe

logs:
	@docker logs debezium --tail 50
