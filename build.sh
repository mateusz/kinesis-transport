#!/bin/bash -x

BUILDDIR=./build

for dir in kinesis*; do
	if [ -d $dir ]; then
		echo "Building $dir"
		cd $dir
		$GOPATH/bin/gox -output="../$BUILDDIR/{{.Dir}}_{{.OS}}_{{.Arch}}" -osarch "linux/amd64 darwin/amd64"
		cd ..
	fi
done
