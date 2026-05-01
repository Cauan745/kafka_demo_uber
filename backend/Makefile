start-dockers:
	docker compose -f docker-compose.yml up -d

dev: start-dockers
	go run cmd/main.go

stop:
	docker compose down

restart-dev: stop dev

QUANTITY = 1 

producer:
	@echo "Running with QUANTITY: $(QUANTITY)"
	go run cmd/producer/driver_tracker.go -quantity=$(QUANTITY)

run-bunch-producers:
	@echo "Running with QUANTITY: $(QUANTITY)"
	seq 1 50 | xargs -I {} -P 50 sh -c 'make producer >/dev/null 2>&1'

driver-logger:
	go run cmd/consumer/driver_logger/driver_logger.go --consumerGroup="local_cg3"
	
area-tracker:
	go run cmd/consumer/area_tracker/area_tracker.go -consumerGroup="local_cg2"

server: start-dockers
	go run cmd/consumer/server/*
