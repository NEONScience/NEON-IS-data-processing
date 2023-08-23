# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=neon-is-thsh-slct-ts-pad
tag=$(git rev parse --short HEAD)
docker build -t $image_name:latest -f ./modules_combined/threshold_select_and_timeseries_padder/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
