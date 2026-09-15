# Setup Script

```bash
python scripts/setup_dev.py
```

One command that prepares a checkout for development:

- creates the [Python virtual environment](contributing.md#python-environment) in `.venv` and installs `scripts/requirements.txt`
- installs the [pre-commit hooks](contributing.md#pre-commit-hooks)
- generates the [sample data](tests.md#fixtures) the tests need
- builds the [documentation](documentation.md)

Run it again at any time to update everything.
