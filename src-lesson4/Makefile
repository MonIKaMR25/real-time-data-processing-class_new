DATE ?= 2024-01-15

.PHONY: up down seed naive prove prove-naive idempotent failure watermark \
        scd2 schema-check schema-drift clean logs ps

up:            ## Build + start the whole stack (postgres, runner, airflow, dagster)
	docker compose up -d --build

down:          ## Stop everything
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

seed:          ## Seed the OLTP source (1M orders, 50k customers, 90 days)
	./bench python src/seed_data.py

naive:         ## Phase 1: run the naive pipeline once
	./bench python src/pipeline_naive.py $(DATE)

prove:         ## Phase 2: prove the idempotent pipeline (3 runs, identical state)
	./bench python src/prove_idempotent.py $(DATE)

prove-naive:   ## Contrast: prove the NAIVE pipeline is NOT idempotent
	./bench python src/prove_idempotent.py $(DATE) --loader naive

idempotent:    ## Phase 2: run the idempotent pipeline once
	./bench python src/pipeline_idempotent.py $(DATE)

failure:       ## Phase 3: failure injection + retry until success
	./bench python src/pipeline_failure.py $(DATE) --retries 8 --fail-prob 0.4

watermark:     ## Phase 4: multi-date load with watermark + atomic metadata
	./bench python src/pipeline_watermark.py 2024-01-10 2024-01-20

scd2:          ## SCD Type 2: initial merge, move customers (+ pinned #42), re-merge, show history
	./bench python src/scd2.py --merge --effective-date 2024-02-01
	./bench python src/scd2.py --simulate-move 500
	./bench python src/scd2.py --move-id 42
	./bench python src/scd2.py --merge --effective-date 2024-03-15
	./bench python src/scd2.py --show 42

schema-check:  ## Validate the source schema against the contract
	./bench python src/schema_validate.py

schema-drift:  ## Demo: rename a source column → validation FAILs loudly → reset (lossless)
	-./bench python src/schema_validate.py --simulate rename
	-./bench python src/schema_validate.py
	./bench python src/schema_validate.py --reset

clean:         ## Delete the analytical target (start fresh)
	rm -f data/analytics.duckdb
	rm -rf data/staging
