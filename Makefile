install: ## 使用 uv 安装项目依赖
	uv sync

lint:
	uv run isort --check .
	uv run black --check .
	uv run flake8 src tests

format: ## 使用 Black 格式化代码
	uv run isort .
	uv run black .

run: ## 使用 uv 运行程序（如定义 entrypoints）
	uv run {{ cookiecutter.package_name }}


test:
	uv run pytest -v tests

publish:
	uv build
	uv publish
