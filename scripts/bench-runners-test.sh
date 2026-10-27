#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
real_go=$(command -v go)
fixture=$(mktemp -d)
on_exit() {
	local status=$?
	if [ "$status" -eq 0 ]; then
		rm -rf "$fixture"
	else
		echo "fixture evidence retained: $fixture" >&2
	fi
}
trap on_exit EXIT
export FIXTURE_ROOT="$fixture/repo"
export FIXTURE_TOOLS="$fixture/tools"
export FIXTURE_REAL_REPO="$repo"
export FIXTURE_REAL_GO="$real_go"
export FIXTURE_ORIGINAL_PATH="$PATH"
"$real_go" -C "$repo" build -o "$fixture/benchmeta" ./scripts/benchmeta
export FIXTURE_HELPER="$fixture/benchmeta"
export FIXTURE_MARKER="$fixture/timed"
mkdir -p "$FIXTURE_ROOT/scripts/benchmeta" "$FIXTURE_ROOT/ring" "$FIXTURE_ROOT/benchmark" \
	"$FIXTURE_TOOLS" "$fixture/home"
cp "$repo/scripts/"{lib.sh,ring-quick-bench.sh,ring-compare-bench.sh} "$FIXTURE_ROOT/scripts/"
cp "$repo/scripts/benchmeta/"*.go "$FIXTURE_ROOT/scripts/benchmeta/"
printf 'module fixture\n' >"$FIXTURE_ROOT/go.mod"
printf 'module fixture/benchmark\n' >"$FIXTURE_ROOT/benchmark/go.mod"
printf 'production fixture\n' >"$FIXTURE_ROOT/ring/ring.go"
printf 'test fixture\n' >"$FIXTURE_ROOT/ring/harness_test.go"
printf 'test fixture\n' >"$FIXTURE_ROOT/benchmark/harness_test.go"
printf compiler >"$FIXTURE_TOOLS/compile"
printf linker >"$FIXTURE_TOOLS/link"

{
	echo 'BenchmarkRingAPI/api=seq/elem=int64'
	for mode in SPSC MPMC; do
		for batch in 1 32; do
			echo "BenchmarkThroughput/mode=$mode/batch=$batch/elem=int64/impl=ring"
		done
	done
	echo 'BenchmarkHandoff/impl=ring'
	for mode in SPSC MPMC; do
		echo "BenchmarkRingAPI/mode=$mode/api=single/elem=int64"
		for api in burst bulk; do
			for batch in 1 8 32 64; do
				echo "BenchmarkRingAPI/mode=$mode/api=$api/batch=$batch/elem=int64"
			done
		done
	done
} >"$fixture/quick.rows"
while read -r implementation modes batches; do
	for mode in ${modes//,/ }; do
		for batch in ${batches//,/ }; do
			for element in int64 ptr; do
				echo "BenchmarkCompare/mode=$mode/batch=$batch/elem=$element/impl=$implementation"
			done
		done
	done
done >"$fixture/compare.rows" <<'ROWS'
ring SPSC,MPSC,SPMC,MPMC 1,32
ring-exact SPSC,MPSC,SPMC,MPMC 1,32
ring-single SPSC,MPSC,SPMC,MPMC 1
ring-rts MPSC,SPMC,MPMC 1,32
frontier MPSC 1,32
frontier-single MPSC 1
chan SPSC,MPSC,SPMC,MPMC 1
mutex SPSC,MPSC,SPMC,MPMC 1
xsync SPSC,MPSC,SPMC,MPMC 1
workiva SPSC,MPSC,SPMC,MPMC 1
ROWS
export FIXTURE_DATA="$fixture"

cat >"$FIXTURE_TOOLS/go" <<'GO'
#!/usr/bin/env bash
set -euo pipefail
args=("$@")
directory=$PWD
if [ "${1:-}" = -C ]; then
	directory=$2
	shift 2
fi
case "$1 ${2:-}" in
'build -o')
	cp "$FIXTURE_HELPER" "$3"
	exit 0
	;;
'env GOARCH') echo "${FIXTURE_ARCH:-amd64}" ;;
'env GOTOOLDIR') echo "$FIXTURE_TOOLS" ;;
'env -json')
	printf '{"GOOS":"%s","GOARCH":"%s","CGO_ENABLED":"0","GOROOT":"%s",' \
		"${FIXTURE_GOOS:-linux}" "${FIXTURE_ARCH:-amd64}" "$FIXTURE_TOOLS"
	echo '"GOTOOLCHAIN":"local","GOWORK":"off","GOFLAGS":"-mod=readonly"}'
	;;
'version -m')
	printf '%s: go fixture\n\tbuild\tGOOS=%s\n\tbuild\tGOARCH=%s\n\tbuild\tCGO_ENABLED=0\n' \
		"$3" "${FIXTURE_GOOS:-linux}" "${FIXTURE_ARCH:-amd64}"
	case "$3" in
	*race*)
		printf '\tbuild\t-race=true\n'
		;;
	esac
	;;
'version ') echo 'go version fixture' ;;
'list -m') echo '{"Path":"fixture","Version":"v1"}' ;;
'list -deps')
	if [ -f "$directory/benchmark/go.mod" ] && [ "${*: -1}" = ./ring ]; then
		package_dir="$directory/ring"
	else
		package_dir="$directory"
	fi
	files='"harness_test.go"'
	extra=''
	[ ! -f "$package_dir/added_test.go" ] || extra=',"added_test.go"'
	[ ! -f "$package_dir/removed" ] || files=''
	if [ -n "$files" ]; then
		files="$files$extra"
	fi
	overlay=''
	for ((i = 0; i < ${#args[@]}; i++)); do
		case "${args[i]}" in
		-overlay) overlay=${args[i + 1]} ;;
		-overlay=*) overlay=${args[i]#-overlay=} ;;
		esac
	done
	if [ -n "$overlay" ]; then
		mapped_original=$(awk -F '"' '$2 == "Replace" { print $4 }' "$overlay")
		if [ "$mapped_original" = "$package_dir/layout_pad_test.go" ]; then
			files="${files:+$files,}\"layout_pad_test.go\""
		fi
	fi
	if [ -f "$package_dir/ring.go" ]; then
		production='"ring.go"'
	else
		production=''
	fi
	printf '{"Dir":"%s","ImportPath":"github.com/freshdresch/go-data-structures/ring",' "$package_dir"
	printf '"GoFiles":[%s],"TestGoFiles":[%s]}\n' "$production" "$files"
	;;
'test -c' | 'test -race')
	out=''
	for ((i = 0; i < ${#args[@]}; i++)); do
		if [ "${args[i]}" = -o ]; then
			out=${args[i + 1]}
		fi
	done
	[ -n "$out" ]
	echo "build: $(basename "$out")" >>"$FIXTURE_DATA/events"
	cp "$FIXTURE_TOOLS/binary" "$out"
	chmod +x "$out"
	mutation=${FIXTURE_MUTATION:-}
	if [ -n "${FIXTURE_MUTATION_VARIANT:-}" ] &&
		[[ "$out" != *"$FIXTURE_MUTATION_VARIANT.test" ]]; then
		mutation=''
	fi
	if [ -d "$directory/ring" ]; then
		mutate_dir="$directory/ring"
	else
		mutate_dir="$directory"
	fi
	case "$mutation" in
	modify) printf changed >>"$mutate_dir/harness_test.go" ;;
	add) printf added >"$mutate_dir/added_test.go" ;;
	remove) touch "$mutate_dir/removed" ;;
	overlay-source) printf changed >>"$(dirname "$out")/layout_pad_test.go" ;;
	overlay-mapping) printf '{"Replace":{}}' >"$(dirname "$out")/overlay.json" ;;
	esac
	if [ -n "$mutation" ]; then
		echo "mutated: $mutation" >>"$FIXTURE_DATA/events"
	fi
	;;
'tool nm')
	binary=${*: -1}
	offset=${FIXTURE_OFFSET:-0}
	case "$binary" in
	*after.test)
		offset=${FIXTURE_AFTER_OFFSET:-$offset}
		;;
	esac
	[ "$offset" != unknown ] || exit 1
	case "$binary" in
	*/compare-padded.test)
		offset=${FIXTURE_PADDED_OFFSET:-0}
		;;
	esac
	printf '%x D runtime.sched\n' "$offset"
	if [ "$2" = nm ] && [ "${3:-}" = -size ]; then
		pad=$(sed -n 's/^var layoutPad \[\([0-9]*\)\].*/\1/p' "$(dirname "$binary")/layout_pad_test.go")
		echo "100 $((8 * pad)) D github.com/freshdresch/go-data-structures/benchmark.layoutPad"
	fi
	;;
*)
	echo "unexpected fixture go: $*" >&2
	exit 9
	;;
esac
GO

cat >"$FIXTURE_TOOLS/binary" <<'BINARY'
#!/usr/bin/env bash
if [ -n "${FIXTURE_EXECUTION_MARKER:-}" ]; then
	echo "$(basename "$0")" >>"$FIXTURE_EXECUTION_MARKER"
fi
set -euo pipefail
cpu=1
run=''
bench=''
count=1
while [ $# -gt 0 ]; do
	case "$1" in
	-test.cpu) cpu=$2 ;;
	-test.run) run=$2 ;;
	-test.bench) bench=$2 ;;
	-test.count) count=$2 ;;
	esac
	shift
done
kind=quick
case "$0" in
*compare*)
	kind=compare
	;;
esac
if [ -n "$bench" ]; then
	echo "$(basename "$0") $bench" >>"$FIXTURE_MARKER"
	[ "${FIXTURE_FAIL_TIMING:-false}" = false ] || exit 7
	awk -v cpu="$cpu" -v kind="$kind" -v bench="$bench" '
  kind == "quick" && bench ~ /api=seq/ && $0 !~ /api=seq/ { next }
  kind == "quick" && bench !~ /api=seq/ && $0 ~ /api=seq/ { next }
  kind == "compare" {
   split($0, parts, "/impl=")
   if (index(bench, "impl=" parts[2] "$" ) == 0) next
  }
  {
   name=$0
   if (cpu != 1) name=name "-" cpu
   printf "%s 129 20 ns/op", name
   if ($0 ~ /^BenchmarkRingAPI\/mode=/) printf " 10 ns/elem 2 elem/op"
   print " 0 B/op 0 allocs/op"
  }' "$FIXTURE_DATA/$kind.rows"
	echo PASS
	exit 0
fi
if [[ "$run" == *TestBenchmarkManifest* ]]; then
	echo '=== RUN   TestBenchmarkManifest'
	awk -v cpu="$cpu" -v wrong="${FIXTURE_WRONG_ROWS:-false}" '
  BEGIN { printf "BENCHMARK_MANIFEST {\"schema\":1,\"gomaxprocs\":%s,\"rows\":[", cpu }
  wrong == "true" && NR == 2 { next }
  {
   printf "%s{\"name\":\"%s\",\"family\":\"fixture\",\"implementation\":\"ring\",", sep, $0
   printf "\"api\":\"burst\",\"api_reason\":\"\",\"element\":\"int64\",\"batch\":1,"
   printf "\"usable_capacity\":1024,\"producers\":1,\"consumers\":1,\"worker_reason\":\"\","
   printf "\"callers\":0,\"participants\":0,\"queue_count\":1,\"ring_options\":[],"
   printf "\"ring_options_reason\":\"\",\"producer_wait\":\"yield\",\"consumer_wait\":\"yield\","
   printf "\"wait_reason\":\"\",\"primary_metric\":\"ns/op\",\"timing_boundary\":\"fixture\","
   printf "\"pointer_pool_size\":null,\"pointer_pool_reason\":\"int64\",\"storage\":\"int64\"}"
   sep=","
  }
  END { print "]}" }' "$FIXTURE_DATA/$kind.rows"
	echo '--- PASS: TestBenchmarkManifest (0.00s)'
	echo PASS
	exit 0
fi
[ "${FIXTURE_FAIL_GATE:-false}" = false ] || exit 8
names=${run#^(}
names=${names%)$}
names=${names//|/ }
for ((iteration = 0; iteration < count; iteration++)); do
	for name in $names; do
		echo "--- PASS: $name (0.00s)"
	done
done
echo PASS
BINARY

cat >"$FIXTURE_TOOLS/git" <<'GIT'
#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = -C ]; then
	shift 2
fi
case "$1 ${2:-}" in
'rev-parse --show-toplevel') echo "$FIXTURE_ROOT" ;;
'rev-parse HEAD') echo fixture-head ;;
'rev-parse --verify') echo "${*: -1}" ;;
'archive '*)
	if [ "${FIXTURE_METHODOLOGY:-false}" = true ] && [[ "$2" == *after* ]]; then
		printf changed-measurement >"$FIXTURE_ROOT/ring/harness_test.go"
	fi
	tar -C "$FIXTURE_ROOT" -cf - .
	printf 'test fixture\n' >"$FIXTURE_ROOT/ring/harness_test.go"
	;;
'diff --stat' | 'status --porcelain' | 'status --porcelain'*) : ;;
*)
	echo "unexpected fixture git $*" >&2
	exit 9
	;;
esac
GIT
cat >"$FIXTURE_TOOLS/uname" <<'UNAME'
#!/bin/sh
case "$1" in
-s)
	echo "${FIXTURE_OS:-Linux}"
	;;
*)
	echo fixture-machine
	;;
esac
UNAME
cat >"$FIXTURE_TOOLS/lscpu" <<'LSCPU'
#!/bin/sh
if [ "$#" = 0 ]; then
	echo 'Model name: fixture'
else
	printf '0,0,0\n1,1,0\n'
fi
LSCPU
cat >"$FIXTURE_TOOLS/taskset" <<'TASKSET'
#!/bin/sh
shift 2
exec "$@"
TASKSET
cat >"$FIXTURE_TOOLS/sysctl" <<'SYSCTL'
#!/bin/sh
case "$2" in
hw.ncpu)
	echo 10
	;;
*)
	echo fixture
	;;
esac
SYSCTL
cat >"$FIXTURE_TOOLS/pmset" <<'PMSET'
#!/bin/sh
echo 'Now drawing from AC Power'
PMSET
cat >"$FIXTURE_TOOLS/benchstat" <<'BENCHSTAT'
#!/bin/sh
echo fixture-benchstat
BENCHSTAT
chmod +x "$FIXTURE_TOOLS/"*
export PATH="$FIXTURE_TOOLS:$PATH"
export HOME="$fixture/home"

run_case() {
	local name=$1
	local expected=$2
	shift 2
	rm -f "$FIXTURE_MARKER" "$FIXTURE_DATA/events"
	local status=0
	"$@" >"$fixture/$name.log" 2>&1 || status=$?
	if [ "$expected" = accepted ]; then
		if [ "$status" -ne 0 ] || [ ! -s "$FIXTURE_MARKER" ]; then
			cat "$fixture/$name.log" >&2
			echo "$name: expected accepted timing fixture, got $status" >&2
			exit 1
		fi
	else
		if [ "$status" -eq 0 ] || [ -e "$FIXTURE_MARKER" ]; then
			cat "$fixture/$name.log" >&2
			echo "$name: expected rejection before timing, got $status" >&2
			exit 1
		fi
	fi
	if [[ "$name" == compare-padded-* ]]; then
		grep -q '^build: compare-padded.test$' "$FIXTURE_DATA/events" || {
			echo "$name: padded variant was never built" >&2
			exit 1
		}
		grep -q '^mutated:' "$FIXTURE_DATA/events" || {
			echo "$name: mutation was never dispatched" >&2
			exit 1
		}
	fi
	if [ "$name" = quick ]; then
		result=$(sed -n 's/^results: //p' "$fixture/$name.log")
		grep -q '^round 1 order: before after$' "$result/state.txt"
		grep -q '^round 2 order: after before$' "$result/state.txt"
	fi
	if [ "$name" = methodology ] || [ "$name" = apple-layout-diagnostic ]; then
		result=$(sed -n 's/^results: //p' "$fixture/$name.log")
		label=methodology-comparison
		if [ "$name" = apple-layout-diagnostic ]; then
			label=layout-confounded
		fi
		grep -q "$label" "$result/comparison.txt"
	fi
	printf '%s: exit=%s expected=%s\n' "$name" "$status" "$expected"
}

quick="$FIXTURE_ROOT/scripts/ring-quick-bench.sh"
compare="$FIXTURE_ROOT/scripts/ring-compare-bench.sh"
run_case quick accepted bash "$quick" -rounds 2 before after
origin=$(find "$HOME/ring-quick" -mindepth 1 -maxdepth 1 -type d | head -n 1)
run_case reuse accepted bash "$quick" -rounds 1 -reuse "$origin"
reuse_result=$(sed -n 's/^results: //p' "$fixture/reuse.log")
for side in before after; do
	cmp "$origin/$side.test" "$reuse_result/$side.test"
	cmp "$origin/provenance/$side.json" "$reuse_result/provenance/$side.json"
	cmp "$origin/provenance/$side.snapshot.json" \
		"$reuse_result/provenance/$side.snapshot.json"
done
for corrupt_side in before after; do
	for override in none methodology layout both; do
		corrupt_origin="$fixture/corrupt-$corrupt_side-$override"
		cp -R "$origin" "$corrupt_origin"
		for side in before after; do
			if [ "$corrupt_side" = before ] || [ "$side" = after ]; then
				{
					head -n 1 "$origin/$side.test"
					# Expand these variables when the generated fixture runs.
					# shellcheck disable=SC2016
					echo 'echo "$(basename "$0")" >>"$FIXTURE_EXECUTION_MARKER"'
					tail -n +2 "$origin/$side.test"
				} >"$corrupt_origin/$side.test"
			fi
		done
		flags=()
		case "$override" in
		methodology) flags=(-methodology-comparison) ;;
		layout) flags=(-allow-layout-confounded) ;;
		both) flags=(-methodology-comparison -allow-layout-confounded) ;;
		esac
		execution_marker="$fixture/executed-$corrupt_side-$override"
		name="reuse-corrupt-$corrupt_side-$override"
		run_case "$name" rejected env FIXTURE_EXECUTION_MARKER="$execution_marker" \
			bash "$quick" -rounds 1 -reuse "$corrupt_origin" ${flags[@]+"${flags[@]}"}
		rejected_result=$(find "$HOME/ring-quick" -name state.txt -exec \
			grep -l "^reuse-parent: $corrupt_origin$" {} +)
		grep -q 'artifact hash mismatch' "$(dirname "$rejected_result")/provenance/admission.json"
		grep -q '^status: aborted at admission$' "$rejected_result"
		if [ -e "$execution_marker" ]; then
			cat "$execution_marker" >&2
			echo "$name: reused binary executed before hash rejection" >&2
			exit 1
		fi
	done
done
run_case wrong-rows rejected env FIXTURE_WRONG_ROWS=true bash "$quick" -rounds 1 before after
for mutation in modify add remove; do
	run_case "mutation-$mutation" rejected env FIXTURE_MUTATION="$mutation" \
		bash "$quick" -rounds 1 before after
done
run_case methodology-rejected rejected env FIXTURE_METHODOLOGY=true \
	bash "$quick" -rounds 1 before after
run_case methodology accepted env FIXTURE_METHODOLOGY=true bash "$quick" \
	-methodology-comparison -rounds 1 before after
run_case apple-layout rejected env FIXTURE_OS=Darwin FIXTURE_GOOS=darwin FIXTURE_ARCH=arm64 \
	FIXTURE_AFTER_OFFSET=96 bash "$quick" -rounds 1 before after
run_case apple-layout-diagnostic accepted env FIXTURE_OS=Darwin FIXTURE_GOOS=darwin \
	FIXTURE_ARCH=arm64 FIXTURE_AFTER_OFFSET=unknown bash "$quick" \
	-allow-layout-confounded -rounds 1 before after
run_case diagnostic-no-methodology rejected env FIXTURE_OS=Darwin FIXTURE_GOOS=darwin \
	FIXTURE_ARCH=arm64 FIXTURE_METHODOLOGY=true FIXTURE_AFTER_OFFSET=96 bash "$quick" \
	-allow-layout-confounded -rounds 1 before after
run_case comparison accepted bash "$compare" -rounds 1
run_case failed-gate rejected env FIXTURE_FAIL_GATE=true bash "$compare" -rounds 1
run_case apple-padding accepted env FIXTURE_OS=Darwin FIXTURE_GOOS=darwin \
	FIXTURE_ARCH=arm64 FIXTURE_OFFSET=96 bash "$compare" -rounds 1
run_case linux-pad rejected bash "$compare" -pad 4 -rounds 1
run_case legacy-reuse rejected bash "$quick" -reuse "$fixture"
for variant in compare-unpadded compare-padded compare-race; do
	for mutation in modify add remove; do
		extra=()
		if [ "$variant" = compare-padded ]; then
			extra=(FIXTURE_OS=Darwin FIXTURE_GOOS=darwin FIXTURE_ARCH=arm64 FIXTURE_OFFSET=96)
		fi
		run_case "$variant-$mutation" rejected env ${extra[@]+"${extra[@]}"} \
			FIXTURE_MUTATION="$mutation" FIXTURE_MUTATION_VARIANT="$variant" \
			bash "$compare" -rounds 1
		printf 'test fixture\n' >"$FIXTURE_ROOT/benchmark/harness_test.go"
		rm -f "$FIXTURE_ROOT/benchmark/added_test.go" "$FIXTURE_ROOT/benchmark/removed"
	done
done
for mutation in overlay-source overlay-mapping; do
	run_case "$mutation" rejected env FIXTURE_OS=Darwin FIXTURE_GOOS=darwin FIXTURE_ARCH=arm64 \
		FIXTURE_OFFSET=96 FIXTURE_MUTATION="$mutation" FIXTURE_MUTATION_VARIANT=compare-padded \
		bash "$compare" -rounds 1
done
rm -f "$FIXTURE_MARKER"
status=0
env FIXTURE_FAIL_TIMING=true bash "$quick" -rounds 1 before after \
	>"$fixture/failed-invocation.log" 2>&1 || status=$?
[ "$status" -ne 0 ] && [ -s "$FIXTURE_MARKER" ]
grep -q 'invocation failed' "$fixture/failed-invocation.log"
echo "failed-invocation: exit=$status raw failure retained"
echo 'runner fixtures complete'
