# Create some data files
echo "hello" > ./file

# Create repo A with file A
pachctl create repo A
pachctl start commit A@master
pachctl put file A@master:/file -f ./file
pachctl finish commit A@master

# Create repo B with file B
pachctl create repo B
pachctl start commit B@master
pachctl put file B@master:/file -f ./file
pachctl finish commit B@master

# Create pipelines symlinkA and symlinkB. These will be our alternate sources for symlinkC.
pachctl create pipeline -f ./symlinkA
pachctl create pipeline -f ./symlinkB

# Create pipeline symlinkC with symlinkA as it's input
pachctl create pipeline -f ./symlinkC

# See that symlinkC runs successfully
# Now go edit the pipeline spec of symlinkC. Change input.pfs.repo from symlinkA to symlinkB

# Send the update to pachyderm
pachctl update pipeline -f ./symlinkC

# Delete symlinkC's previous input, symlinkA
pachctl delete pipeline symlinkA

# Update data in B
pachctl start commit B@master
pachctl finish commit B@master
