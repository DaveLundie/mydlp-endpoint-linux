#!/bin/bash

REVISION=1

if [ -n "$1" ]; then
	REVISION=$1
fi

CURRENTVERSION=$(git describe |sed -s 's/-.*$//')
#CURRENTVERSION=$(git tag|tail -1)
NEXTVERSION=""

function incrementVersion {
        PRE=$(echo $CURRENTVERSION|sed -e 's/\.[0-9]\+$//'|sed -e 's/^v//')
        LAST=$(echo $CURRENTVERSION|sed -e 's/^v[0-9]\+\.[0-9]\+\.//')
        NEXTVERSION="$PRE"".""$(( $LAST + 1 ))"
}

function applyTemplate {
	local TMPL=$1
	local OUT=$2
	local TMPFILE="$OUT""~"

	cp -af $TMPL $TMPFILE
	sed -i -e "s/%%%VERSION%%%/$NEXTVERSION/g" $TMPFILE
	sed -i -e "s/%%%REVISION%%%/$REVISION/g" $TMPFILE
	mv -f $TMPFILE $OUT
}

incrementVersion

echo "Current version is $NEXTVERSION-$REVISION"

applyTemplate configure.ac.tmpl configure.ac
applyTemplate debian/changelog.tmpl debian/changelog

echo $NEXTVERSION > package_version
echo $REVISION > package_revision

git submodule update --init --recursive

cp -a engine.Makefile.am src/engine/src/mydlp/Makefile.am

autoreconf -f -i

