#!/bin/bash -x

BUILDDIR=./build
LATEST_TAG=`git describe --abbrev=0 --tags`

rm -rf ${BUILDDIR} && mkdir -p ${BUILDDIR}

for dir in kinesis*; do
	if [ -d $dir ]; then
		echo "Building $dir"
		cd $dir
		$GOPATH/bin/gox -output="../$BUILDDIR/{{.Dir}}_${LATEST_TAG}_{{.OS}}_{{.Arch}}" -osarch "linux/amd64 darwin/amd64"
		cd ..
	fi
done
