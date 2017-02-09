EXTENSION = brian

MODULE_big = brian
OBJS = brian.o queries.pb-c.o

DATA = brian--0.0.1.sql

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

SHLIB_LINK += -lprotobuf-c

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
