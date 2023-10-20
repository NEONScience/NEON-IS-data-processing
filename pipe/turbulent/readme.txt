default turbulent_group_path 91bab5384edd4a32a037d04e0b118adf 4 hours ago About an hour 0       446 + 0 / 446 42.59GiB 0B failure: duplicate path output by...

to look at full failure information,
pachctl inspect job turbulent_group_path@91bab5384edd4a32a037d04e0b118adf

turbulent_group_path job failed due to duplicate path output if we include all of the turbulent sensors.
it was caused by gascylinder(gasRefe) and presValiRegInTurb share the same CFGLOC location, which ends up the same group/CFGLOC103678.json file.

Instead of modifying module to change the output location for group file (group/CFGLOGxxx.json),
we handle the issue in pachyderm pipelines and organize the input files differently.

The initial input was:

join:
  - pfs: (GROUP_ASSIGNMENT_PATH)
  - union:
    - pfs: (LOCATION_FOCUS_PATH)
    - pfs: (LOCATION_FOCUS_PATH)
    - pfs: (LOCATION_FOCUS_PATH)
    - pfs: ... ...

1)
union:
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - pfs: (LOCATION_FOCUS_PATH_1)
    - pfs: (LOCATION_FOCUS_PATH_2)
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - pfs: (LOCATION_FOCUS_PATH_3)
    - pfs: (LOCATION_FOCUS_PATH_4)
    - pfs:
    - pfs:
    - pfs:
2)
union:
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - pfs: (LOCATION_FOCUS_PATH_1)
    - pfs: (LOCATION_FOCUS_PATH_2)
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - pfs: (LOCATION_FOCUS_PATH)
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - pfs: (LOCATION_FOCUS_PATH)
- join:
    - pfs:
    - pfs:
3)
uion:
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - pfs: (LOCATION_FOCUS_PATH_1)
    - pfs: (LOCATION_FOCUS_PATH_2)
- join:
    - pfs: (GROUP_ASSIGNMENT_PATH)
    - union:
        - pfs: (LOCATION_FOCUS_PATH)
        - pfs: (LOCATION_FOCUS_PATH)
        - pfs:
        - pfs:
        - pfs:
        - pfs:




A few things tried as in turbulent_group_path_zly(_*).yaml, 1 and 3 worked fine for full data, 
though I never finished testing for 2, it takes forever for pachyderm to figure out how many datums

first option (in turbulent_group_path_zly.yaml) takes longer for one day test but worked well for "full" data in turbulent_group_path,
which group sensors sharing same location together, and group all other sensors together, then union two groups
#---------
  cmd:
  - sh
  - "-c"
  - |-
    /bin/bash <<'EOF'
    # Handle duplicate group path
    rm -r /tmp/LOCATION_FOCUS_PATH
    rm -r /tmp/LOCATION_FOCUS_PATH_part2
    if [[ $(echo $LOCATION_FOCUS_PATH_1) ]]; then
      mkdir /tmp/LOCATION_FOCUS_PATH
      cp -r /pfs/LOCATION_FOCUS_PATH_1/* /tmp/LOCATION_FOCUS_PATH
      cp -r /pfs/LOCATION_FOCUS_PATH_2/* /tmp/LOCATION_FOCUS_PATH
      export LOCATION_FOCUS_PATH=/tmp/LOCATION_FOCUS_PATH
      python3 -m group_path.group_path_main
    elif [[ $(echo $LOCATION_FOCUS_PATH_3) ]]; then
      mkdir /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_3/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_4/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_5/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_6/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_7/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_8/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_9/* /tmp/LOCATION_FOCUS_PATH_part2
      cp -r /pfs/LOCATION_FOCUS_PATH_10/* /tmp/LOCATION_FOCUS_PATH_part2
      export LOCATION_FOCUS_PATH=/tmp/LOCATION_FOCUS_PATH_part2
      python3 -m group_path.group_path_main
    fi
    EOF
---
input:
  union:
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
          repo: turbulent_group_assignment
          glob: /turbulent/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_1
          repo: gascylinder_l0p_data
          glob: /gasRefe/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_2
          repo: presValiRegInTurb_analyze_pad_and_qaqc_plau
          glob: /presValiRegInTurb/(*/*/*)
          joinOn: $1
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
          repo: turbulent_group_assignment
          glob: /turbulent/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_3
          repo: mfcSampTurb_analyze_pad_and_qaqc_plau
          glob: /mfcSampTurb/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_4
          repo: mfcValiTurb_analyze_pad_and_qaqc_plau
          glob: /mfcValiTurb/(*/*/*)
          joinOn: $1
      - pfs:
#-----------

#################################
option 2 in turbulent_group_path_zly_5.yaml, which takes 5 minutes to run 1 day data (2020/01/04)
but it didn't work well for "full" data, which takes forever to figure out how many datum for pipelines
#-----------
    # Handle duplicate group path
    rm -r /tmp/LOCATION_FOCUS_PATH
    if [[ $(echo $LOCATION_FOCUS_PATH_1) ]]; then
      mkdir /tmp/LOCATION_FOCUS_PATH
      cp -r /pfs/LOCATION_FOCUS_PATH_1/* /tmp/LOCATION_FOCUS_PATH
      cp -r /pfs/LOCATION_FOCUS_PATH_2/* /tmp/LOCATION_FOCUS_PATH
      export LOCATION_FOCUS_PATH=/tmp/LOCATION_FOCUS_PATH
      python3 -m group_path.group_path_main
    else
      python3 -m group_path.group_path_main
    fi
    EOF
----
  union:
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
          repo: turbulent_group_assignment
          glob: /turbulent/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_1
          repo: gascylinder_l0p_data
          glob: /gasRefe/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_2
          repo: presValiRegInTurb_analyze_pad_and_qaqc_plau
          glob: /presValiRegInTurb/(*/*/*)
          joinOn: $1
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
          repo: turbulent_group_assignment
          glob: /turbulent/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH
          repo: mfcSampTurb_analyze_pad_and_qaqc_plau
          glob: /mfcSampTurb/(*/*/*)
          joinOn: $1
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
#------------


#############################################
the other option takes 11 minutes with 1 restart to finish for 1 day's data,
didn't check with "full" data set. it has one more layer of union to group all other sensors than option 1
#------------
  union:
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
          repo: turbulent_group_assignment_test
          glob: /turbulent/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_1
          repo: gascylinder_l0p_data
          glob: /gasRefe/(*/*/*)
          joinOn: $1
      - pfs:
          # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
          name: LOCATION_FOCUS_PATH_2
          repo: presValiRegInTurb_analyze_pad_and_qaqc_plau
          glob: /presValiRegInTurb/(*/*/*)
          joinOn: $1
  - join:
      - pfs:
          # name must be GROUP_ASSIGNMENT_PATH
          name: GROUP_ASSIGNMENT_PATH
          repo: turbulent_group_assignment_test
          glob: /turbulent/(*/*/*)
          joinOn: $1
      - union:
        - pfs:
            # Any/all repos in location focus name must be named LOCATION_FOCUS_PATH
            name: LOCATION_FOCUS_PATH
            repo: mfcSampTurb_analyze_pad_and_qaqc_plau
            glob: /mfcSampTurb/(*/*/*)
            joinOn: $1
        - pfs:
#------------
