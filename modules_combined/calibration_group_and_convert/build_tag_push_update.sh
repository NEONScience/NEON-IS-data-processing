# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=neon-is-cal-grp-conv
tag=$(git rev-parse HEAD)
docker build -t $image_name:latest -f ./modules_combined/calibration_group_and_convert/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"