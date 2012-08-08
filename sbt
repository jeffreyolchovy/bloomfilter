java -Xmx512M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -jar `dirname $0`/sbt-launch.jar "$@"
