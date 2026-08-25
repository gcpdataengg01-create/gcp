param(
    [Parameter(Mandatory=$true)][string]$ProjectId,
    [Parameter(Mandatory=$true)][string]$Region,
    [Parameter(Mandatory=$true)][string]$ComposerEnvironment
)

$ErrorActionPreference = "Stop"

# Seven source business dates, all in a December peak period. The normal DAG is
# triggered seven times; there is no backfill-only DAG or transformation path.
$BusinessDates = @(
    "2010-12-06",
    "2010-12-07",
    "2010-12-08",
    "2010-12-09",
    "2010-12-10",
    "2010-12-13",
    "2010-12-14"
)

foreach ($BusinessDate in $BusinessDates) {
    $RunId = "backfill-$($BusinessDate.Replace('-', ''))-$(Get-Date -Format 'yyyyMMddHHmmss')"
    $Conf = '{"business_date":"' + $BusinessDate + '"}'

    gcloud composer environments run $ComposerEnvironment `
      --project $ProjectId `
      --location $Region `
      dags trigger -- retail_batch_etl `
      --run-id $RunId `
      --conf $Conf

    Write-Host "Triggered $RunId for $BusinessDate"
}
