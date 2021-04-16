$ROOT = "C:\"
$WORKING_DIR = $ROOT + "Production\"
$STAGE = $WORKING_DIR + "stage\"
$GPG_PROG = "${env:ProgramFiles(x86)}" + "\GNU\GnuPG\gpg.exe"

foreach ($f in (gci -Path ($STAGE + "*.zip"))) {
    & $GPG_PROG -o ($f.FullName + ".pgp") -r "${KEY_ID}" -e ($f.FullName)
    Rename-Item $f.FullName ($f.Name -Replace "^", "${STRING}_")
}