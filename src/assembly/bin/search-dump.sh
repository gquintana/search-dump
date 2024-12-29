#!/usr/bin/env sh
echo "Search Dump ${VERSION}"
basedir="$(dirname $0)/.."
java -jar "${basedir}/lib/search-dump-${VERSION}.jar" $*