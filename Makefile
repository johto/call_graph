# call_graph/Makefile

MODULES = call_graph

EXTENSION = call_graph
DATA = call_graph--1.0--1.1.sql call_graph--1.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
