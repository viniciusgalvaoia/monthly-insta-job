prepare:
	@black .
	@isort .
	@mypy job
	@pylint job
	@flake8 job
	@echo Good to Go!

check:
	@black . --check
	@isort . --check
	@mypy job
	@flake8 job
	@pylint job
	@echo Good to Go!

docs:
	@mkdocs build --clean

docs-serve:
	@mkdocs serve

test:
	@pytest --cov job

test-cov:
	@pytest --cov job --cov-report xml:coverage.xml
.PHONY: docs
