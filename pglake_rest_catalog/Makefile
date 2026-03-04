PROGRAM = pglake_rest_catalog
PG_CONFIG ?= pg_config
PG_BINDIR := $(shell $(PG_CONFIG) --bindir)

GOFLAGS ?=

.PHONY: all install clean check

all:
	cd cmd/pglake-rest-catalog && go build $(GOFLAGS) -o ../../$(PROGRAM) .

install: all
	install $(PROGRAM) $(DESTDIR)$(PG_BINDIR)/$(PROGRAM)

clean:
	rm -f $(PROGRAM)
	go clean ./...

check:
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

installcheck:
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
