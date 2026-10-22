.PHONY: help up down logs network fleet seed-network report

include make/data.mk
include make/stream.mk
include make/app.mk

help:
	@python -c "print('Targets: up down logs network fleet seed-network train-ml report')"
