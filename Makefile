.PHONY: update-format format check test test-dist build types upload cov clean clean-env

package=hpc_funcs
version_file1=./src/hpc_funcs/version.py

VERSION=$(shell cat ${version_file1} | egrep -o "([0-9]{1,}\.)+[0-9]{1,}")
VERSION_PATCH=$(shell echo ${VERSION} | cut -d'.' -f3)
VERSION_MINOR=$(shell echo ${VERSION} | cut -d'.' -f2)
VERSION_MAJOR=$(shell echo ${VERSION} | cut -d'.' -f1)
GIT_COMMIT=$(shell git rev-parse --short HEAD)

env=env
python=${env}/bin/python
pytest=${env}/bin/pytest

all: env

## Setup

env: ${env}_uv

env_uv:
	uv venv ${env}
	uv pip install -e . --python ${env}/bin/python
	uv pip install -e .[dev] --python ${env}/bin/python
	${python} -m pre_commit install

## Development

update-format:
	${python} -m pre_commit autoupdate

format:
	${python} -m pre_commit run --all-files

check: format types

test:
	${python} -m pytest ./tests

test-dist:
	${python} -m twine check dist/*

types:
	${python} -m monkeytype run $$(which ${pytest}) ./tests
	${python} -m monkeytype list-modules | grep ${package} | parallel -j1 "${python} -m monkeytype apply {} > /dev/null && echo {}"

cov:
	${python} -m pytest --cov=${package} --cov-config .coveragerc --cov-report html tests

build:
	${python} -m build --skip-dependency-check  .

upload:
	${python} -m twine upload ./dist/*

start-jupyter:
	${python} -m jupyterlab

## Version

version:
	echo ${VERSION}

bump-version-dev:
	test ! -z "${VERSION}"
	test ! -z "${GIT_COMMIT}"
	exit 1 # Not Implemented

bump-version-patch:
	test ! -z "${VERSION_PATCH}"
	make set-version VERSION=${VERSION_MAJOR}.${VERSION_MINOR}.$(shell awk 'BEGIN{print ${VERSION_PATCH}+1}')

bump-version-minor:
	test ! -z "${VERSION_MINOR}"
	make set-version VERSION=${VERSION_MAJOR}.$(shell awk 'BEGIN{print ${VERSION_MINOR}+1}').0

bump-version-major:
	test ! -z "${VERSION_MAJOR}"
	make set-version VERSION=$(shell awk 'BEGIN{print ${VERSION_MAJOR}+1}').0.0

set-version:
	test ! -z "${VERSION}"
	sed -i 's/\(^\|.*:\)__version__ = .*/__version__ = "${VERSION}"/' ${version_file1}

commit-tag-version:
	@# git tag --list | grep -qix "${VERSION}"
	git commit -m "Version ${VERSION}" --no-verify ${version_file1}
	git tag '${package}-${VERSION}'

## Github


## Clean

clean:
	find ./src/ -type f \
		-name "*.so" \
		-name "*.pyc" \
		-name ".pyo" \
		-delete
	rm -rf ./src/*.egg-info/
	rm -rf *.whl
	rm -rf ./build/ ./__pycache__/
	rm -rf ./dist/

clean-env:
	rm -rf ./${env}/
	rm -f .git/hooks/pre-commit
