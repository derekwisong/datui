
# Running the Tests

Running the tests is done using Cargo's test command.

```bash
cargo test
```

However, the tests require sample data which are too large to add to the repo. Instead,
the data must be generated before the tests can be run.

## Generating Sample Data

> If you used the [Setup Script](contributing.md#setup-script), the sample data has already
> been generated. To regenerate the data, see the [instructions](tests.md#regenerating-or-updating-the-sample-data)

The tests will automatically run a Python script to generate the sample files if
they do not already exist. However, that script has some dependencies.

To install the dependencies, I recommend following the
[Python Virtual Environment Setup Instructions](contributing.md#python-virtual-environment)
from the [Contributing](contributing.md) section.

Once you have a Python virtual environment set up with the `requirements.txt` from
the `scripts/` directory, and activated it, you're ready to run the tests for the first time.

```bash
# activate the virtual environment if sample data is not already generated
source .venv/bin/activate

# run the tests
cargo test
```

The tests will look for the files and run the generation script if they don't already exist.
Having the virtual environment activated before running tests for the first time ensures the
automatic generation goes smoothly.

After the files are built you don't need to have that environment activated anymore to run tests.

## Regenerating or Updating the Sample Data

You can run the data generation script yourself:
```bash
python scripts/generate_sample_data.py
```

> The data will not be automatically regenerated in the future. Use the script to regenerate
> the data when necessary.