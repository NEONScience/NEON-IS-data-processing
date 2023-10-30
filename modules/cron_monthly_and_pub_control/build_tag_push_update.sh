# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=neon-is-pub-cntl
tag=$(git rev-parse --short HEAD)
cd ./modules
docker build -t $image_name:latest -f ./cron_monthly_and_pub_control/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag
cd ..
Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
