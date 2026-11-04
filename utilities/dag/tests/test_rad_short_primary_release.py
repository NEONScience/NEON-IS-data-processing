import calendar
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest
import yaml

from filter_joiner.joiner import FilterJoiner
from group_path import group_path_main
from level1_consolidate import level1_consolidate_main


ROOT = Path(__file__).resolve().parents[3]
PIPE_PATHS = [
    *sorted((ROOT / "pipe/cmp22").glob("*_release.yaml")),
    *sorted((ROOT / "pipe/radShortPrimary").glob("*_release.yaml")),
]
SPECS = {
    path.stem: yaml.safe_load(path.read_text())
    for path in PIPE_PATHS
}


def spec(stage):
    return SPECS[f"radShortPrimary_{stage}_release"]


def script(pipeline):
    transform = pipeline["transform"]
    return "\n".join(transform["stdin"]) if "stdin" in transform else transform["cmd"][-1]


def pfs_inputs(node):
    if "pfs" in node:
        yield node["pfs"]
    for kind in ("cross", "join", "union", "group"):
        for child in node.get(kind, []):
            yield from pfs_inputs(child)


def run_shell(source, env):
    return subprocess.run(
        ["bash", "-euo", "pipefail"],
        input=source, text=True, capture_output=True,
        env={**os.environ, **env},
    )


def test_release_dag_is_isolated_and_pipeline_lists_are_topological():
    seen = {"cmp22_site_list"}
    listed = set()
    for folder in ("cmp22", "radShortPrimary"):
        listing = ROOT / f"pipe/{folder}/pipe_list_{folder}_release.txt"
        for filename in listing.read_text().splitlines():
            pipeline = SPECS[Path(filename).stem]
            name = pipeline["pipeline"]["name"]
            assert name == Path(filename).stem
            assert name.endswith("_release")
            for pfs in pfs_inputs(pipeline["input"]):
                assert pfs["repo"] in seen
            seen.add(name)
            listed.add(name)
    assert listed == set(SPECS)
    assert len(listed) == 15
    for stage in ("location_loader",):
        pipeline = SPECS[f"cmp22_{stage}_release"]
        assert pipeline["input"]["pfs"]["glob"] == "/tick/"
    for stage in ("group_loader", "srf_loader"):
        assert spec(stage)["input"]["pfs"]["glob"] == "/tick/"
    for stage in ("pub_group", "pub_format_and_package", "pub_egress_and_publish"):
        regular = yaml.safe_load((ROOT / f"pipe/radShortPrimary/radShortPrimary_{stage}.yaml").read_text())
        expected_env = regular["transform"]["env"].copy()
        if stage == "pub_group":
            expected_env["ERR_PATH_GROUPER"] = "/pfs/out/errored_datums"
        assert spec(stage)["transform"]["env"] == expected_env
        assert spec(stage)["transform"]["image"] == regular["transform"]["image"]
        assert spec(stage)["transform"].get("secrets") == regular["transform"].get("secrets")


@pytest.mark.parametrize("pipeline", list(SPECS.values()), ids=list(SPECS))
def test_release_bash_syntax(pipeline):
    subprocess.run(["bash", "-n"], input=script(pipeline), text=True, check=True)


@pytest.mark.parametrize(
    "start,end,valid",
    [
        ("2026-04-01", "2026-06-30", True),
        ("2024-02-01", "2024-02-29", True),
        ("2026-12-01", "2027-01-31", True),
        ("2026-04-13", "2026-06-30", False),
        ("2026-04-01", "2026-06-12", False),
        ("2026-07-01", "2026-06-30", False),
        ("2026-02-01", "2026-02-29", False),
    ],
)
def test_daily_control_requires_complete_months(start, end, valid):
    control = SPECS["cmp22_cron_daily_and_date_control_release"]
    result = run_shell(control["transform"]["stdin"][2], {"START_DATE": start, "END_DATE": end})
    assert (result.returncode == 0) == valid, result.stderr


def test_daily_control_builds_selected_dates_and_refreshes_metadata_tick(tmp_path):
    pipeline = SPECS["cmp22_cron_daily_and_date_control_release"]
    sites = tmp_path / "sites.json"
    sites.write_text(json.dumps([
        {"site": "CPER", "start_date": "2020-01-01"},
        {"site": "BONA", "start_date": "2020-01-01", "kafka_start_date": "2024-02-15"},
    ]))
    output = tmp_path / "out"
    source = script(pipeline).replace("/pfs/out", str(output))
    env = {
        **pipeline["transform"]["env"],
        "START_DATE": "2024-02-01", "END_DATE": "2024-02-29",
        "OUT_PATH": str(output), "SITE_FILE": str(sites),
    }
    result = run_shell(source, env)
    assert result.returncode == 0, result.stderr
    assert (output / "data_year_2024.txt").read_text() == "2024\n"
    assert len(list((output / "cmp22/2024/02").iterdir())) == 29
    assert (output / "cmp22/2024/02/01/CPER").read_text() == "CPER\n"
    assert (output / "cmp22/2024/02/14/BONA").is_file()
    assert (output / "cmp22/2024/02/15/BONA.kafka").is_file()
    tick = (output / "tick/tick").read_text()
    result = run_shell(source, env)
    assert result.returncode == 0, result.stderr
    assert (output / "tick/tick").read_text() != tick


@pytest.mark.parametrize("year,month", [(2024, 2), (2026, 4), (2026, 12)])
def test_monthly_control_rejects_missing_dates(tmp_path, year, month):
    dates = tmp_path / f"cmp22/{year}/{month:02d}"
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        (dates / f"{day:02d}").mkdir(parents=True)
    output = tmp_path / "out"
    source = script(spec("cron_monthly_and_pub_control")).replace("/pfs/out", str(output))
    result = run_shell(source, {"DATE_PATH": str(dates)})
    assert result.returncode == 0, result.stderr
    assert (output / f"{year}/{month:02d}/.empty").is_file()
    (dates / "02").rmdir()
    result = run_shell(source, {"DATE_PATH": str(dates)})
    assert result.returncode != 0
    assert "missing day 02" in result.stderr


@pytest.mark.parametrize("pipeline_name", [
    "cmp22_location_active_dates_assignment_release",
    "radShortPrimary_group_assignment_release",
    "radShortPrimary_srf_assignment_release",
])
@pytest.mark.parametrize("month,following", [
    ("2024/02", "2024-03-01"),
    ("2026/04", "2026-05-01"),
    ("2026/12", "2027-01-01"),
])
def test_assignments_use_selected_month_and_exclusive_end(tmp_path, pipeline_name, month, following):
    binary = tmp_path / "Rscript"
    binary.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$TEST_ARGS"\n')
    binary.chmod(0o755)
    arguments = tmp_path / "arguments"
    result = run_shell(script(SPECS[pipeline_name]), {
        "DATE_PATH": f"/pfs/DATE_PATH/cmp22/{month}",
        "DIR_IN": "/pfs/DIR_IN/metadata/member",
        "TEST_ARGS": str(arguments),
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
    })
    assert result.returncode == 0, result.stderr
    args = arguments.read_text().splitlines()
    assert f"DateBgn={month.replace('/', '-')}-01" in args
    assert f"DateEnd={following}" in args
    assert not any(arg.startswith("FileYear=") for arg in args)


@pytest.mark.parametrize("case", ["success", "missing", "gcs-error", "bad-partition"])
def test_reingest_restores_archive_paths_and_surfaces_errors(tmp_path, case):
    archive = tmp_path / "archive"
    archive.mkdir()
    expected = []
    if case != "missing":
        for interval in ("001", "030"):
            group = "rad-short-primary_CPER001000"
            filename = f"{group}_2026-04-01_SWR_1min_{interval}.parquet"
            source = archive / interval / f"group={group}/ms=2026-04" / filename
            source.parent.mkdir(parents=True)
            source.write_bytes(b"archived final table")
            expected.append(Path("2026/04/01") / group / "data" / filename)
        other_day = source.with_name(filename.replace("2026-04-01", "2026-04-02"))
        other_day.write_bytes(b"not selected")
        if case == "bad-partition":
            source.rename(source.with_name(filename.replace("_030.parquet", "_001.parquet")))
    binary = tmp_path / "bin/rclone"
    binary.parent.mkdir()
    binary.write_text(
        f"#!{sys.executable}\n"
        "import fnmatch, os, pathlib, shutil, sys\n"
        "if os.environ['CASE'] == 'gcs-error': sys.exit(9)\n"
        "args = sys.argv[1:]\n"
        "destination = pathlib.Path(args[args.index('copy') + 2])\n"
        "pattern = args[args.index('--include') + 1]\n"
        "root = pathlib.Path(os.environ['ARCHIVE'])\n"
        "for source in root.rglob('*.parquet'):\n"
        "    relative = source.relative_to(root)\n"
        "    if fnmatch.fnmatchcase('/' + relative.as_posix(), pattern):\n"
        "        target = destination / relative\n"
        "        target.parent.mkdir(parents=True, exist_ok=True)\n"
        "        shutil.copyfile(source, target)\n"
    )
    binary.chmod(0o755)
    output = tmp_path / "out"
    source = script(spec("level1_reingest")).replace("/pfs/out", str(output))
    result = run_shell(source, {
        "DATE_PATH": "/pfs/DATE_PATH/cmp22/2026/04/01",
        "GROUP_PREFIX": "rad-short-primary",
        "BUCKET_NAME": "test-bucket",
        "ARCHIVE": str(archive),
        "CASE": case,
        "PATH": f"{binary.parent}:{os.environ['PATH']}",
    })
    if case == "success":
        assert result.returncode == 0, result.stderr
        assert {path.relative_to(output) for path in output.rglob("*.parquet")} == set(expected)
        assert all((output / path).read_bytes() == b"archived final table" for path in expected)
    else:
        assert result.returncode != 0
        if case == "missing":
            assert "No archived radShortPrimary L1 tables" in result.stderr
        elif case == "bad-partition":
            assert "disagrees with archive partition" in result.stderr


@pytest.mark.parametrize("with_srf", [False, True])
def test_group_metadata_join_and_consolidation_contract(fs, monkeypatch, with_srf):
    group = "rad-short-primary_CPER001000"
    day_group = Path("2026/04/01") / group
    group_json = {"features": [{"properties": {"group": group}}]}
    fs.create_file(
        "/pfs/GROUP_ASSIGNMENT_PATH/rad-short-primary/2026/04/01/CFGLOC1/group/member.json",
        contents=json.dumps(group_json),
    )
    fs.create_file(
        "/pfs/LOCATION_FOCUS_PATH/cmp22/2026/04/01/CFGLOC1/location/location.json",
        contents="fresh location",
    )
    group_env = spec("group_path")["transform"]["env"]
    for key, value in group_env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("GROUP_ASSIGNMENT_PATH", "/pfs/GROUP_ASSIGNMENT_PATH")
    monkeypatch.setenv("LOCATION_FOCUS_PATH", "/pfs/LOCATION_FOCUS_PATH")
    monkeypatch.delenv("GROUP_FOCUS_PATH", raising=False)
    group_path_main.main()
    assert (Path("/pfs/out") / day_group / "group/member.json").is_file()
    assert (Path("/pfs/out") / day_group / "cmp22/CFGLOC1/location/location.json").is_file()
    fs.rename("/pfs/out", "/pfs/GROUP_PATH")
    table = day_group / "data" / f"{group}_2026-04-01_SWR_1min_001.parquet"
    fs.create_file(Path("/pfs/LEVEL1_PATH") / table, contents="unchanged L1")
    join_env = spec("filter_joiner")["transform"]["env"]
    FilterJoiner(
        config=join_env["CONFIG"], out_path=Path("/pfs/RELEASE_PATH"),
        relative_path_index=int(join_env["RELATIVE_PATH_INDEX"]), symlink=False,
    ).join()
    assert (Path("/pfs/RELEASE_PATH") / table).read_text() == "unchanged L1"

    srf_name = "science_review_flags/srf.json"
    if with_srf:
        fs.create_file(Path("/pfs/SRF_PATH") / day_group / srf_name, contents="new SRF")
        fs.create_file(Path("/pfs/SRF_PATH/2026/04/02") / group / srf_name, contents="wrong day")
    release = spec("level1_group_consolidate_srf")
    joined = Path("/tmp/radshort-release.test/joined")
    FilterJoiner(
        config=release["transform"]["env"]["CONFIG"],
        out_path=joined, relative_path_index=3, symlink=False,
    ).join()
    assert (joined / table).read_text() == "unchanged L1"
    assert (joined / day_group / srf_name).exists() == with_srf
    assert not (joined / "2026/04/02").exists()
    for key, value in release["transform"]["env"].items():
        monkeypatch.setenv(key, value)
    indices = dict(re.findall(r"\b([A-Z_]+)=(\d+)", script(release)))
    assert indices == {
        "RELATIVE_PATH_INDEX": "4", "GROUP_INDEX": "7",
        "GROUP_METADATA_INDEX": "8", "DATA_TYPE_INDEX": "10",
    }
    for key, value in indices.items():
        monkeypatch.setenv(key, value)
    consolidated = Path("/tmp/radshort-release.test/pfs/consolidated")
    monkeypatch.setenv("IN_PATH", str(joined))
    monkeypatch.setenv("OUT_PATH", str(consolidated))
    level1_consolidate_main.main()
    assert (consolidated / table).read_text() == "unchanged L1"
    assert (consolidated / day_group / "location/location.json").read_text() == "fresh location"
    assert (consolidated / day_group / "group/member.json").is_file()
    assert (consolidated / day_group / srf_name).exists() == with_srf


@pytest.mark.parametrize("missing", ["group", "location"])
def test_filter_joiner_does_not_accept_missing_metadata(fs, missing):
    day_group = Path("2026/04/01/rad-short-primary_CPER001000")
    fs.create_file(Path("/pfs/LEVEL1_PATH") / day_group / "data/table_001.parquet")
    if missing != "group":
        fs.create_file(Path("/pfs/GROUP_PATH") / day_group / "group/member.json")
    if missing != "location":
        fs.create_file(Path("/pfs/GROUP_PATH") / day_group / "cmp22/CFGLOC1/location/location.json")
    config = spec("filter_joiner")["transform"]["env"]["CONFIG"]
    FilterJoiner(config=config, out_path=Path("/pfs/out"), relative_path_index=3, symlink=False).join()
    assert not (Path("/pfs/out") / day_group / "data/table_001.parquet").exists()


@pytest.mark.parametrize("missing_site", [False, True])
def test_mdp_publication_routes_each_site_and_rejects_unknown_buckets(tmp_path, missing_site):
    output = tmp_path / "out"
    product = "NEON.DOM.SITE.DP1.00022.001"
    for site in ("MD01", "MD02"):
        (output / "mdp" / product / site).mkdir(parents=True)
    config = tmp_path / "mdp-config"
    config.write_text(
        "MD01 false true bucket-one/Publication\n" +
        ("" if missing_site else "MD02\tfalse\ttrue\tbucket-two/Publication\n")
    )
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for command, body in {
        "curl": 'cp "$TEST_MDP_CONFIG" "$OUT_MDP_SITES"',
        "rclone": 'printf "%s\\n" "$*" >> "$TEST_COPIES"',
        "python3": 'printf "%s\\n" "$*" >> "$TEST_MODULES"',
    }.items():
        binary = binaries / command
        binary.write_text(f"#!/bin/sh\n{body}\n")
        binary.chmod(0o755)
    env = spec("pub_egress_and_publish")["transform"]["env"].copy()
    env.update(
        OUT_PATH=str(output), OUT_PATH_MDP=str(output / "mdp"),
        OUT_MDP_SITES=str(tmp_path / "mdp-sites"),
        DATA_PATH="/pfs/DATA_PATH", BUCKET_NAME="regular-publication",
        TEST_MDP_CONFIG=str(config), TEST_COPIES=str(tmp_path / "copies"),
        TEST_MODULES=str(tmp_path / "modules"),
        PATH=f"{binaries}:{os.environ['PATH']}",
    )
    result = run_shell(script(spec("pub_egress_and_publish")), env)
    if missing_site:
        assert result.returncode != 0
        assert "No MDP bucket configured for MD02" in result.stderr
        assert "pub_uploader" not in (tmp_path / "modules").read_text()
    else:
        assert result.returncode == 0, result.stderr
        copies = (tmp_path / "copies").read_text().splitlines()
        assert len(copies) == 2
        assert copies[0].endswith(f":gcs://bucket-one/Publication/{product}/MD01")
        assert copies[1].endswith(f":gcs://bucket-two/Publication/{product}/MD02")
