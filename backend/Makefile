start-dockers:
	docker compose -f docker-compose.yml up -d

dev: start-dockers
	go run cmd/main.go

stop:
	docker compose down

restart-dev: stop dev

producer:
	go run cmd/producer/driver_tracker.go

run-bunch-producers:
	seq 1 300 | xargs -I {} -P 300 sh -c 'make producer >/dev/null 2>&1'

driver-logger:
	go run cmd/consumer/driver_logger/driver_logger.go --consumerGroup="local_cg3"
	
area-tracker:
	go run cmd/consumer/area_tracker/area_tracker.go -consumerGroup="local_cg2"

ws-server:
	go run cmd/consumer/server/server.go
