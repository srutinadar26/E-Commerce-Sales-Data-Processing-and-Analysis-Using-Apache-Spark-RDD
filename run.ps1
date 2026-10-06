# run.ps1 - Helper script for Windows
#
# Usage:
#   .\run.ps1              -> runs the full pipeline
#   .\run.ps1 tests        -> runs pytest
#   .\run.ps1 generate     -> generates the dataset only

param([string]$Command = "pipeline")

# ------------------------------------------------------------
# Java configuration
# ------------------------------------------------------------

$javaHome = "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"

if (-not (Test-Path "$javaHome\bin\java.exe")) {
    Write-Error "Temurin JDK 21 not found at: $javaHome"
    Write-Error "Please verify the Java installation path."
    exit 1
}

$env:JAVA_HOME = $javaHome
$env:PATH = "$javaHome\bin;$env:PATH"

# ------------------------------------------------------------
# Python / PySpark configuration
# ------------------------------------------------------------

$pythonExe = "$PWD\.venv311\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python 3.11 virtual environment not found:"
    Write-Error $pythonExe
    exit 1
}

$env:PYSPARK_PYTHON = $pythonExe
$env:PYSPARK_DRIVER_PYTHON = $pythonExe
$env:PYTHONIOENCODING = "utf-8"

# Python 3.11 is being used, so these are not required
# for the Python 3.12 Windows workaround.
$env:PYSPARK_NO_DAEMON = "1"
$env:PYSPARK_PIN_THREAD = "true"

Write-Host "JAVA_HOME: $env:JAVA_HOME" -ForegroundColor Cyan
Write-Host "PySpark Python: $env:PYSPARK_PYTHON" -ForegroundColor Cyan
Write-Host ""

& java -version 2>&1 | Write-Host
& $pythonExe --version

# ------------------------------------------------------------
# Run requested command
# ------------------------------------------------------------

switch ($Command.ToLower()) {

    "pipeline" {
        Write-Host "`nRunning full pipeline..." -ForegroundColor Green
        & $pythonExe scripts/run_pipeline.py
    }

    "generate" {
        Write-Host "`nGenerating dataset..." -ForegroundColor Green
        & $pythonExe scripts/generate_data.py
    }

    "tests" {
        Write-Host "`nRunning tests..." -ForegroundColor Green
        & $pythonExe -m pytest tests/ -v
    }

    default {
        Write-Host "Unknown command: $Command"
        Write-Host "Usage: .\run.ps1 [pipeline|generate|tests]"
    }
}