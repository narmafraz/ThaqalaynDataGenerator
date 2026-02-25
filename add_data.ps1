$Env:PYTHONPATH = "$PSScriptRoot;$PSScriptRoot/app"
$Env:DESTINATION_DIR = "../ThaqalaynData/"
$Env:SOURCE_DATA_DIR = "../ThaqalaynDataSources/"

# Run with uv (uses project's virtual environment)
uv run python .\app\main_add.py
