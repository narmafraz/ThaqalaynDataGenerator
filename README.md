# ThaqalaynDataGenerator

Parses data from various sources and populates data in ThaqalaynData repo for the Thaqalayn app.

## Quick Start

1. Install uv: `curl -LsSf https://astral.sh/uv/install.sh | sh` (or see https://docs.astral.sh/uv/)
2. Install dependencies: `uv sync`
3. Set up output directory: Ensure `../ThaqalaynData/` exists
4. Run generator: `./add_data.ps1` (Windows) or set PYTHONPATH/DESTINATION_DIR and run `uv run python app/main_add.py`

See [CLAUDE.md](CLAUDE.md) for detailed documentation.
