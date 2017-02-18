EXTENSION = micromanage

MODULE_big = micromanage
OBJS = micromanage.o queries.pb-c.o

DATA = micromanage--0.0.1.sql queries.proto

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

SHLIB_LINK += -lprotobuf-c

REGRESS=projection sorting joins

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

queries.pb-h.h queries.pb-c.c: queries.proto
	protoc-c --c_out=. queries.proto

micromanage.o: queries.pb-h.h

#check: temp-install
#	$(prove_check)
