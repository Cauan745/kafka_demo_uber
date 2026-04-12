start-dockers:
	docker compose -f docker-compose.yml up -d

dev: start-dockers
	go run cmd/main.go

stop:
	docker compose down

restart-dev: stop dev

producer:
	go run cmd/producer/driver_tracker.go

driver-logger:
	go run cmd/consumer/driver_logger/driver_logger.go
	
area-tracker:
	go run cmd/consumer/area_tracker/area_tracker.go -consumerGroup="local_cg2"
