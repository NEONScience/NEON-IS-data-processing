#!/usr/bin/env bash
# 
# This script will build the docker image and update the image reference in other Dockerfiles throughout the repo. 
# This script WILL NOT build downstream images in the /pack directory that reference this image.  
# This script WILL build downstream images that reference this image in the /flow, /modules, and 
#     /modules_combined directories and update references to those images in pipeline specs in the 
#     /pipe directory. 
#
# **** BEFORE RUNNING ****
# 1. Commit all changes
# 2. Run this script from root repository (NEON-IS-data-processing)
# ************************

image_name=neon-is-pack-pub-r

cd ./pack/NEONprocIS.pub

# Create tag with package version and commit short SHA
commit=$(git rev-parse --short HEAD)
version_line=$(cat DESCRIPTION |grep Version:) # snag the line with the package version
version=${version_line#"Version: "}
tag="v$version-$(git rev-parse --short HEAD)" # Compile image tag

docker build --no-cache -t $image_name:latest .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag
cd ../..

Rscript ./utilities/flow.img.updt.R "./" "Dockerfile" "quay.io/battelleecology/$image_name" "$tag" "TRUE"
