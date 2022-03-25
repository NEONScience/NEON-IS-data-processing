#!/bin/bash
# Run interactively

# Make some test files
echo "hello1" >> ./testFile1
echo "hello1" >> ./testFile2

# Put the first test file into a repo
pachctl create repo trigger
pachctl start commit trigger@master
pachctl put file trigger@master -f ./testFile1
pachctl finish commit trigger@master

# Deploy a pipeline that just copies the file to the output, with the datum set as the whole repo
pachctl create pipeline -f ./symlink.json

# Let the job finish and check the output - you see testFile1 in the output
pachctl glob file symlink@master:/**

# Now swap testFile1 for testFile2 in the input repo. Same contents, different file name
pachctl start commit trigger@master
pachctl delete file -r trigger@master:/
pachctl put file trigger@master -f ./testFile2
pachctl finish commit trigger@master

# Let the job finish and check the output - you STILL see testFile1 in the output
pachctl glob file symlink@master:/**