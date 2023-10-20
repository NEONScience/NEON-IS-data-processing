# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash

## this is a TEMPERORY repo
image_name=neon-sae-trst-dp0p-python

tag=$(git rev parse --short HEAD)
cd ./flow/flow.sae.ecte.l1l4
docker build -t quay.io/battelleecology/$image_name:$tag .
docker push quay.io/battelleecology/$image_name:$tag
cd ../..

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
