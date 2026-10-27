#!/usr/bin/env python3
"""Fetch pinned official etcd fixtures without installing system software.

Release archives are verified against upstream SHA256SUMS. When an official
native archive is absent (notably etcd 3.4 on darwin/arm64), --build-missing
builds the resolved tag commit in an isolated source directory. The provenance
file records the source commit, downloaded archive hash, toolchain, and binary
hash. Source archive hashes are recorded evidence, not published signatures.
"""

import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile


DEFAULT_VERSIONS = ("3.4.45", "3.5.34", "3.6.0")
REPOSITORY = "https://github.com/etcd-io/etcd"
API = "https://api.github.com/repos/etcd-io/etcd"


def request(url):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": "etcd-trend-compat-fixture"}),
        timeout=90,
    )


def download(url, path):
    digest = hashlib.sha256()
    with request(url) as response, path.open("xb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
            digest.update(chunk)
    return digest.hexdigest()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_tag(version):
    with request(API + "/git/ref/tags/v" + version) as response:
        obj = json.load(response)["object"]
    for _ in range(4):
        if obj["type"] == "commit":
            return obj["sha"]
        if obj["type"] != "tag":
            break
        with request(API + "/git/tags/" + obj["sha"]) as response:
            obj = json.load(response)["object"]
    raise RuntimeError("tag does not resolve to a commit: " + version)


def release_binary(version, os_name, arch, destination):
    extension = "zip" if os_name == "darwin" else "tar.gz"
    stem = f"etcd-v{version}-{os_name}-{arch}"
    archive_name = stem + "." + extension
    release_url = REPOSITORY + "/releases/download/v" + version
    checksums_path = destination / "SHA256SUMS"
    download(release_url + "/SHA256SUMS", checksums_path)
    expected = None
    for line in checksums_path.read_text().splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1].lstrip("*") == archive_name:
            expected = fields[0]
    if expected is None:
        return None
    archive = destination / archive_name
    actual = download(release_url + "/" + archive_name, archive)
    if actual != expected:
        raise RuntimeError("release archive checksum mismatch: " + archive_name)
    binary = destination / "etcd"
    if extension == "zip":
        with zipfile.ZipFile(archive) as bundle:
            with bundle.open(stem + "/etcd") as source, binary.open("xb") as target:
                shutil.copyfileobj(source, target)
    else:
        with tarfile.open(archive) as bundle:
            source = bundle.extractfile(stem + "/etcd")
            if source is None:
                raise RuntimeError("release archive lacks etcd binary")
            with source, binary.open("xb") as target:
                shutil.copyfileobj(source, target)
    binary.chmod(0o700)
    return {"mode": "official-release", "archive_url": release_url + "/" + archive_name,
            "archive_sha256": actual, "checksum_source": release_url + "/SHA256SUMS"}


def source_binary(version, destination, output):
    commit = resolve_tag(version)
    source_url = f"https://codeload.github.com/etcd-io/etcd/tar.gz/{commit}"
    archive = destination / "source.tar.gz"
    digest = download(source_url, archive)
    source_root = destination / "source"
    source_root.mkdir()
    with tarfile.open(archive) as bundle:
        # Extract only regular files/directories under the expected archive root.
        # Symlinks and special files are unnecessary for the server build.
        for member in bundle.getmembers():
            parts = pathlib.PurePosixPath(member.name).parts
            if not parts or parts[0] != "etcd-" + commit or ".." in parts:
                raise RuntimeError("unexpected source archive path")
            target = source_root.joinpath(*parts[1:])
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = bundle.extractfile(member)
                with source, target.open("xb") as file:
                    shutil.copyfileobj(source, file)
    env = os.environ.copy()
    env.update({"GOWORK": "off", "CGO_ENABLED": "0", "GOTOOLCHAIN": "local"})
    env.setdefault("GOCACHE", str(output / ".go-build"))
    env.setdefault("GOMODCACHE", str(output / ".go-mod"))
    version_output = subprocess.check_output(["go", "version"], env=env, text=True).strip()
    module = source_root
    # v3.4 uses the repository root module; later versions have a server module.
    if (source_root / "server" / "go.mod").exists():
        module = source_root / "server"
        package = "./"
        version_package = "go.etcd.io/etcd/api/v3/version"
    else:
        package = "."
        version_package = "go.etcd.io/etcd/version"
    subprocess.run(["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
                    "-ldflags", f"-X {version_package}.GitSHA={commit[:7]}",
                    "-o", str(destination / "etcd"), package],
                   cwd=module, env=env, check=True)
    return {"mode": "source-build", "source_url": source_url, "source_commit": commit,
            "source_archive_sha256": digest, "build_toolchain": version_output}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=pathlib.Path, help="fixture cache directory")
    parser.add_argument("--versions", default=",".join(DEFAULT_VERSIONS), help="comma-separated release versions")
    parser.add_argument("--build-missing", action="store_true", help="build native source if upstream has no matching binary")
    args = parser.parse_args()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    os_name = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64", "AMD64": "amd64"}.get(platform.machine())
    if not os_name or not arch:
        parser.error("supported fixture hosts are Linux or macOS on amd64/arm64")
    results = []
    for version in args.versions.split(","):
        if not re.fullmatch(r"3\.[456]\.\d+", version):
            parser.error("expected an etcd 3.4, 3.5, or 3.6 patch release: " + version)
        destination = output / ("v" + version)
        if destination.exists():
            info = json.loads((destination / "provenance.json").read_text())
            if sha256(destination / "etcd") != info["binary_sha256"]:
                raise RuntimeError("existing fixture binary checksum differs: " + str(destination))
            results.append(info)
            continue
        with tempfile.TemporaryDirectory(prefix=".fetch-", dir=output) as temporary:
            stage = pathlib.Path(temporary)
            info = release_binary(version, os_name, arch, stage)
            if info is None:
                if not args.build_missing:
                    raise RuntimeError(f"no official {os_name}/{arch} binary for {version}; use --build-missing")
                info = source_binary(version, stage, output)
            env = os.environ.copy()
            env["ETCD_UNSUPPORTED_ARCH"] = arch
            inspected = subprocess.check_output([str(stage / "etcd"), "--version"], env=env, text=True, stderr=subprocess.STDOUT).strip()
            if not re.search(r"(?m)^etcd Version: " + re.escape(version) + r"\s*$", inspected):
                raise RuntimeError("downloaded binary reported an unexpected version")
            info.update({"version": version, "platform": os_name + "/" + arch,
                         "binary_sha256": sha256(stage / "etcd"), "version_output": inspected,
                         "binary": str(destination / "etcd")})
            (stage / "provenance.json").write_text(json.dumps(info, indent=2) + "\n")
            stage.rename(destination)
            results.append(info)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
