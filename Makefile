LINT_DIRS = src # Replace with your actual directories

lint:
	pylint $(LINT_DIRS)
	mypy $(LINT_DIRS)
