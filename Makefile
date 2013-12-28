# call_graph/Makefile

MODULES = call_graph

EXTENSION = call_graph
DATA = call_graph--2.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
