param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

$python = $env:ID2221_PYTHON
if (-not $python -and $env:CONDA_PREFIX) {
    $python = Join-Path $env:CONDA_PREFIX "python.exe"
}

$javaHome = $env:ID2221_JAVA_HOME
if (-not $javaHome -and $env:CONDA_PREFIX) {
    $javaHome = Join-Path $env:CONDA_PREFIX "Library"
}
if (-not $python -or -not (Test-Path $python)) {
    throw "Activate the id2221-week1 Conda environment first, or set ID2221_PYTHON to its python.exe path."
}
if (-not $javaHome -or -not (Test-Path (Join-Path $javaHome "bin\java.exe"))) {
    throw "OpenJDK 17 was not found. Activate the Conda environment or set ID2221_JAVA_HOME to the JDK home."
}

$env:JAVA_HOME = $javaHome
$env:HADOOP_HOME = Join-Path $root "tools\hadoop"
$env:PYTHONPATH = Join-Path $root "src"
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
$env:SPARK_LOCAL_IP = "127.0.0.1"
$sparkTmp = Join-Path $root "tools\spark-tmp"
New-Item -ItemType Directory -Force -Path $sparkTmp | Out-Null
$env:SPARK_LOCAL_DIRS = $sparkTmp
$env:TMP = $sparkTmp
$env:TEMP = $sparkTmp
$env:Path = (Join-Path $env:HADOOP_HOME "bin") + ";" + $env:Path

Set-Location $root
& $python -m urban_data $Command @Args
