$ROOT = "C:\"
$WORKING_DIR = $ROOT + "Production\"
$STAGE = $WORKING_DIR + "stage\"
$LOG = $WORKING_DIR + "logs\"

$NOW = (Get-Date).ToString('yyyy-MM-dd HH.mm.ss')
[xml]$VENDORS = Get-Content ($WORKING_DIR + "vendors.xml")

cd $STAGE

function get-LastProcessDate {
    param($day)
    if (!$day) {
        $day = [DateTime]::Today.AddDays(-5).ToString("yyyy-MM-dd")
    }
    $day = [DateTime]::ParseExact($day, "yyyy-MM-dd", [cultureinfo]::InvariantCulture)
    Write-Output $day
}

foreach ($v in $VENDORS.Vendors.Vendor) {
    $LastProcessDate = (get-LastProcessDate -day ($v.LastProcessDate))
    $EndDate = ([DateTime]::Today.AddDays(-1))
    $DaysBack = New-TimeSpan -Start $LastProcessDate -End $EndDate

    $counter = 0

    while ($counter -ne $DaysBack.Days) {
        $counter++
        $DateOfData = $LastProcessDate.AddDays($counter)
        (python ($ROOT + "helper.py") -s P2 -d $DateOfData.ToString("yyyy-MM-dd") -b $v.BrokerName -c "Production" -r True -x True -v SERVER) >> "${LOG}EXCEPTIONS_${NOW}.txt"
        if (Test-Path ($STAGE + $DateOfData.ToString("yyyy.MM.dd") + "_" + $v.BrokerGUID + "*.zip")) { $v.LastProcessDate = $DateOfData.ToString("yyyy-MM-dd") }
    }
}
gci "*.txt" | Remove-Item
$VENDORS.Save($WORKING_DIR + "vendors.xml")