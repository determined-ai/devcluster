all: check

.PHONY: check
check:
	black --check devcluster
	flake8 devcluster
	mypy devcluster

.PHONY: fmt
fmt:
	black devcluster

.PHONY: clean
clean:
	rm -rf build/ dist/ *.egg-info

.PHONY: build
build:
	$(MAKE) clean
	python3 setup.py -q sdist bdist_wheel

.PHONY: publish
publish: build
	twine check dist/*
	twine upload --verbose --non-interactive dist/*

.PHONY: test-publish
test-publish: build
	twine check dist/*
	twine upload --verbose --non-interactive --repository-url https://test.pypi.org/legacy/ dist/*
