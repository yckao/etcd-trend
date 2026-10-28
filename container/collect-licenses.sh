#!/bin/sh
# Preserve the downloaded Go modules' license/notice files alongside binaries.
set -eu
destination=$1
mkdir -p "$destination/go"
go_root=$(go env GOROOT)
go_license="$go_root/LICENSE"
# Homebrew keeps its license next to libexec; official builder images use GOROOT.
if [ ! -f "$go_license" ]; then go_license="$go_root/../LICENSE"; fi
cp "$go_license" "$destination/go/LICENSE"
module_list=$(mktemp)
trap 'rm -f "$module_list" "$module_list.sorted"' EXIT HUP INT TERM
go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}} {{.Version}} {{.Dir}}{{end}}{{end}}' . > "$module_list"
sort -u "$module_list" > "$module_list.sorted"
printf 'module\tversion\n' > "$destination/modules.tsv"
while read -r module version directory; do
    [ -n "$module" ] || continue
    [ -n "$directory" ] || continue
    target="$destination/modules/$module@$version"
    mkdir -p "$target"
    copied=false
    for name in "$directory"/LICENSE* "$directory"/LICENCE* "$directory"/NOTICE* "$directory"/COPYING* "$directory"/AUTHORS* "$directory"/license* "$directory"/licence* "$directory"/notice* "$directory"/copying*; do
        [ -f "$name" ] || continue
        cp "$name" "$target/"
        copied=true
    done
    if [ "$copied" != true ]; then
        printf 'Missing module license/notice: %s\n' "$module" >&2
        exit 1
    fi
    printf '%s\t%s\n' "$module" "$version" >> "$destination/modules.tsv"
done < "$module_list.sorted"
