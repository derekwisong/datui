# Setup Script

The setup process can be automated by running:

```bash
python scripts/setup-dev.py
```

The script will:

- Set up a [Python Virtual Environment](contributing.md#python-virtual-environment)
- Set up [pre-commit hooks](contributing.md#pre-commit-hooks)
- Generate [sample data](tests.md#regenerating-or-updating-the-sample-data) needed to run the tests
- Configure and build the [documentation](documentation.md)

Run the script again at any time to update everything.
