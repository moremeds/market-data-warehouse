set appBundlePath to POSIX path of (path to me)
set launcherDir to do shell script "/usr/bin/dirname " & quoted form of appBundlePath
set macosDir to do shell script "/usr/bin/dirname " & quoted form of launcherDir
set runnerPath to macosDir & "/scripts/build_and_launch_local_app.sh"

try
	set logPath to do shell script "/bin/zsh " & quoted form of runnerPath
	display notification "Market Data Warehouse launched." with title "Market Data Warehouse"
on error errMsg number errNum
	display dialog "Market Data Warehouse failed to build or launch." & return & return & errMsg buttons {"OK"} default button "OK" with icon stop
end try
