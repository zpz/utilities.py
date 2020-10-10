all:


# 'install' does not require `build` to be run first.
# It does everything and does not leave garbage files behind.
install: FORCE
	pip install --user .


# `build` may be useful because it prints out details
# of the compiling process.
build: FORCE
	python setup.py build


publish: FORCE
	python setup.py sdist bdist_wheel && twine check dist/* && twine upload dist/*


clean: FORCE
	rm -rf build dist
	rm -rf build
	rm -rf src/python/*egg-info
	rm -rf .pytest_cache
	pip uninstall coyote -y


FORCE:
