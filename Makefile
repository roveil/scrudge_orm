reqs:
	uv pip compile --generate-hashes pyproject.toml --extra dev -o uv.lock
	uv pip sync uv.lock

ruff:
	ruff format . --no-cache
	ruff check . --extend-select I --fix --no-cache
