# tap-rawpostgresql

`tap-rawpostgresql` is a Singer tap for PostgreSQL.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.


## Installation

Point your meltano config to this repo

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| username            | True     | None    |             |
| password            | True     | None    |             |
| host                | True     | None    |             |
| port                | True     | None    |             |
| database            | True     | None    |             |
| schema              | True     | None    |             |
| streams             | True     | None    |             |
| batch_size          | False    |  100000 | Size of batch files |
| batch_config        | False    | None    |             |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-rawpostgresql --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-rawpostgresql` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-rawpostgresql --version
tap-rawpostgresql --help
tap-rawpostgresql --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_rawpostgresql/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-rawpostgresql` CLI interface directly using `poetry run`:

```bash
poetry run tap-rawpostgresql --help
```

### Running lint

```bash
poetry run tox -e lint
```

### Format your code

```bash
poetry run tox -e format
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-rawpostgresql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-rawpostgresql --version
# OR run a test `elt` pipeline:
meltano elt tap-rawpostgresql target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
