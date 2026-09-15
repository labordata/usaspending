# Build one Parquet file per (award type, fiscal year) from USAspending's
# monthly award-archive zips.
#
#   make contracts/FY2024.parquet
#   make FY=2024 contracts          # same thing
#   make contracts                  # every FY from $(FIRST_FY) to $(LAST_FY)
#
# The upstream zip name carries a monthly date stamp; scripts/latest_file.py
# resolves the current one and the download is saved under a stable name, with
# the real name in a .source sidecar so the Parquet metadata can record it.

PYTHON ?= .venv/bin/python
FIRST_FY ?= 2008
LAST_FY ?= $(shell date -v+3m +%Y 2>/dev/null || date -d '+3 months' +%Y)
FISCAL_YEARS := $(shell seq $(FIRST_FY) $(LAST_FY))
# files.usaspending.gov serves a WAF block page to curl's default user agent.
UA := Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36

.PHONY: all contracts assistance
all : contracts

contracts : $(foreach fy,$(FISCAL_YEARS),contracts/FY$(fy).parquet)
assistance : $(foreach fy,$(FISCAL_YEARS),assistance/FY$(fy).parquet)

.SECONDARY:

contracts/FY%.parquet : raw/FY%_All_Contracts_Full.zip columns.yml scripts/build.py
	$(PYTHON) scripts/build.py --type contracts --zip $< --out $@ \
	    --source-name "$$(cat $<.source 2>/dev/null || basename $<)"
	cat $<.source > $(basename $@).source

# Assistance keeps only records to identified organizations (record type 2):
# the other 60% of rows are county-level aggregates with no recipient, or
# PII-redacted payments to individuals -- nothing to tie to an employer.
assistance/FY%.parquet : raw/FY%_All_Assistance_Full.zip columns.yml scripts/build.py
	$(PYTHON) scripts/build.py --type assistance --zip $< --out $@ \
	    --where "record_type_code = '2'" \
	    --source-name "$$(cat $<.source 2>/dev/null || basename $<)"
	cat $<.source > $(basename $@).source

raw/FY%_All_Contracts_Full.zip :
	mkdir -p raw
	$(PYTHON) scripts/latest_file.py $* contracts > $@.latest
	curl -sSL -A "$(UA)" --retry 5 --retry-all-errors -C - -o $@ "$$(cut -f2 $@.latest)"
	cut -f1 $@.latest > $@.source && rm $@.latest

raw/FY%_All_Assistance_Full.zip :
	mkdir -p raw
	$(PYTHON) scripts/latest_file.py $* assistance > $@.latest
	curl -sSL -A "$(UA)" --retry 5 --retry-all-errors -C - -o $@ "$$(cut -f2 $@.latest)"
	cut -f1 $@.latest > $@.source && rm $@.latest
