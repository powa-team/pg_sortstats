EXTENSION    = pg_sortstats
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG    ?= pg_config

MODULE_big = pg_sortstats
OBJS = pg_sortstats.o pg_sortstats_import.o

all:

release-zip: all
	git archive --format zip --prefix=${EXTENSION}-$(EXTVERSION)/ --output ./${EXTENSION}-$(EXTVERSION).zip HEAD
	unzip ./${EXTENSION}-$(EXTVERSION).zip
	rm ./${EXTENSION}-$(EXTVERSION).zip
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./${EXTENSION}-$(EXTVERSION)/META.json
	zip -r ./${EXTENSION}-$(EXTVERSION).zip ./${EXTENSION}-$(EXTVERSION)/
	rm ./${EXTENSION}-$(EXTVERSION) -rf


DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
