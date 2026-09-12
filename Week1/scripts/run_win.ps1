param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

$condaEnv = "id2221-week1"
$python = "D:\anaconda3\envs\$condaEnv\python.exe"
if (-not (Test-Path $python)) {
    throw "Conda env '$condaEnv' not found. Run: conda create -n id2221-week1 python=3.12 openjdk=17 -y"
}

$env:JAVA_HOME = "D:\anaconda3\envs\$condaEnv\Library"
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
