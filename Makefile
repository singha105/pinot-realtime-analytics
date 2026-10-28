# make demo: build the Java tools, start a fresh Kafka + Pinot stack, run the benchmark, write results.
JAR := target/pinot-rt.jar

.PHONY: demo build test up down bench logs clean

demo: build down up bench

build:
	./mvnw -B -q package

test:
	./mvnw -B test

up:
	docker compose up -d
	scripts/wait-healthy.sh

down:
	docker compose down -v --remove-orphans

bench:
	scripts/bench.sh

logs:
	docker compose logs --tail 100

clean: down
	./mvnw -B -q clean
