CMD /C .\clean.bat

DIR /B /S src\*.java > .java_files

javac -d bin -g:none --release 8 @.java_files

DEL .java_files
