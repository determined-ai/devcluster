all: check

check:
	black --check devcluster
	flake8 devcluster
	mypy devcluster

fmt:
	black devcluster
