$ROOT = "C:\"
$WORKING_DIR = $ROOT + "Production\"
$STAGE = $WORKING_DIR + "stage\"
$LOG = $WORKING_DIR + "logs\"
Add-Type -Path "C:\Program Files (x86)\WinSCP\WinSCPnet.dll"

$NOW = (Get-Date).ToString('yyyy-MM-dd HH.mm.ss')
$transferOptions = New-Object WinSCP.TransferOptions
$transferOptions.ResumeSupport.State = [WinSCP.TransferResumeSupportState]::Off

## Internal SFTP Folder
$internal_sessionOptions = New-Object WinSCP.SessionOptions -Property @{
    Protocol = [WinSCP.Protocol]::Sftp
    HostName = "${INTERNAL_SFTP}"
    Username = "${INTERNAL_USR}"
    Password = "${INTERNAL_PWD}"
}
try {
    $session = New-Object WinSCP.Session
    $session.SessionLogPath = "${LOG}Internal_Upload_${NOW}.log"
    try {
        $session.Open($internal_sessionOptions)
        $session.PutFiles(
            $STAGE + "*",
            "${INTERNAL_REMOTE_DIR}",
            $True,
            $transferOptions
        )
    }
    catch {
        Write-Host "Error: $($_.Exception.Message)"
        $session.Close()
    }
    finally {
        $session.Close()
    }
}
catch {
    Write-Host "Error: $($_.Exception.Message)"
    exit 1
}