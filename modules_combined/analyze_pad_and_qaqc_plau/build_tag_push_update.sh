# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=neon-is-ts-pad-anls-qaqc-plau
tag=$(git rev-parse --short HEAD)
docker build -t $image_name:latest -f ./modules_combined/analyze_pad_and_qaqc_plau/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
