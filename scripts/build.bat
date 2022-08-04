SET scriptpath=%~dp0

CMD /C %scriptpath%clean.bat

DIR /B /S %scriptpath%..\src\*.java > %scriptpath%..\.java_files

javac -d %scriptpath%..\bin -g:none --release 8 @.java_files

DEL %scriptpath%..\.java_files
