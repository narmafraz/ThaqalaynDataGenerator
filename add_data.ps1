$Env:PYTHONPATH = "$PSScriptRoot;$PSScriptRoot/app"
$Env:DESTINATION_DIR = "../ThaqalaynData/"

python .\data\main_add.py
