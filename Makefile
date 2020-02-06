.PHONY: clean fmt lint test dev_install sanity_check

clean:
	@rm -rf .tox .mypy_cache .pytest_cache dist/ build/ *.egg-info */__pycache__/

fmt:
	black .
	isort --apply

lint:
	flake8 . --count --statistics
	mypy .

dev_install:
	pip3 install --upgrade pip
	pip3 install --pre -e .
	pip3 install -r dev-requirements.txt

sanity_check:
	pip3 list
