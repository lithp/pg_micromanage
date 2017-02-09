EXTENSION = brian

MODULE_big = brian
OBJS = brian.o

DATA = brian--0.0.1.sql

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
