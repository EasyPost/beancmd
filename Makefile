VIRTUALENV := $(if $(GITHUB_ACTIONS),python -m venv,/opt/python3.9/bin/virtualenv)
EP_ENVIRONMENT ?= local
WITHENV = $(if $(GITHUB_ACTIONS),,withenv)

help:
	@cat Makefile | grep '^## ' | cut -c4- | sed -e 's/ - /\t- /;' | column -s "$$( echo -e '\t' )" -t

## install - Fully install the service locally
install: venv/requirements_installed

## clean - Remove the virtual environment, drop all databases and clear out .pyc files
clean:
	rm -rf ~/.venv/beancmd/ venv
	find . -name '*.pyc' -delete

## venv - Install the virtual environment
venv/bin/pip:
ifeq ($(EP_ENVIRONMENT), local)
	$(VIRTUALENV) ~/.venv/beancmd/
	ln -snfT ~/.venv/beancmd/ venv
else
	$(VIRTUALENV) venv
endif

venv/requirements_installed: venv/bin/pip requirements.txt setup.py
	$< install -e . -r requirements.txt
	touch "$@"

venv/test_requirements_installed: venv/bin/pip requirements-tests.txt
	$< install -r requirements-tests.txt
	touch "$@"

## run - Run the HTTP service locally
run: venv/requirements_installed
	${WITHENV} venv/bin/python -m beancmd.beancmd "$@"

## test - Run unit tests
test: venv/requirements_installed venv/test_requirements_installed
ifeq ($(EP_ENVIRONMENT), test)
	withenv ./venv/bin/py.test --cov beancmd --cov-report=term-missing --timeout=30 --junit-xml="${EPCI_JUNIT_TEST_RESULTS}/pytest.xml" tests/
else
	withenv ./venv/bin/py.test --timeout=30 --cov-report=term-missing --cov=beancmd tests/
endif
	venv/bin/flake8 beancmd/ tests/

.PHONY: clean help install run test

# vim:noexpandtab:ts=8
