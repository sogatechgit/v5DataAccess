@echo off
setlocal EnableDelayedExpansion
set id=%1
set pId=%id:"=%


if [%1]==[] (
	echo.
	set /p id="Please enter commit message: "
	set /p sync="Synchronize Backup [y/n]?: "
	set pId=!id!
) else (
	set sync=y
)





set tmp="!pId!"

if %tmp%=="" (
	echo.
	echo Message required !
	echo.
	pause .
) else (
	git add .

	git commit -m "!pId!"
	if "!sync!"=="y" (
		git push -u origin master
	)
)
endlocal
:exit