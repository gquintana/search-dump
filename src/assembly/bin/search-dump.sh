#!/usr/bin/env sh
echo "Search Dump ${VERSION}"
basedir="$(dirname $0)/.."
if [ "$JAVA_HOME" == "" ]; then
  java_cmd="java"
else
  java_cmd="${JAVA_HOME}/bin/java"
fi
$java_cmd -jar "${basedir}/lib/search-dump-${VERSION}.jar" $*