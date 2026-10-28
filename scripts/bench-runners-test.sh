#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
real_go=$(command -v go)
fixture=$(mktemp -d)
# The crossbeam signal cases add the pids they start, so a failing case leaves none running.
case_pids=()
on_exit() {
	local status=$?
	local pid

	for pid in "${case_pids[@]}"; do
		kill -KILL "$pid" 2>/dev/null || true
	done
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
cp "$repo/scripts/"{lib.sh,ring-quick-bench.sh,ring-compare-bench.sh,crossbeam-bench.sh} \
	"$FIXTURE_ROOT/scripts/"
cp "$repo/scripts/benchmeta/"*.go "$FIXTURE_ROOT/scripts/benchmeta/"
printf 'module fixture\n' >"$FIXTURE_ROOT/go.mod"
printf 'module fixture/benchmark\n' >"$FIXTURE_ROOT/benchmark/go.mod"
printf 'production fixture\n' >"$FIXTURE_ROOT/ring/ring.go"
printf 'test fixture\n' >"$FIXTURE_ROOT/ring/harness_test.go"
printf 'test fixture\n' >"$FIXTURE_ROOT/benchmark/harness_test.go"
mkdir -p "$FIXTURE_ROOT/benchmark/crossbeam/src"
printf 'pub const MESSAGES: usize = 5_000_000;\n' >"$FIXTURE_ROOT/benchmark/crossbeam/src/lib.rs"
cat >"$FIXTURE_ROOT/benchmark/crossbeam/Cargo.lock" <<'LOCK'
[[package]]
name = "crossbeam-channel"
version = "0.5.17"
LOCK
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
frontier MPSC,SPMC,MPMC 1,32
frontier-single MPSC,SPMC,MPMC 1
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
	case "$3" in
	*/bin/gochan | */bin/ring) cp "$FIXTURE_TOOLS/crossbeam-program" "$3" ;;
	*) cp "$FIXTURE_HELPER" "$3" ;;
	esac
	exit 0
	;;
'test -tags') : ;;
'doc ./crossbeam/workloads') echo 'const Messages = 5_000_000' ;;
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
	*/bin/gochan | */bin/ring)
		printf '\tbuild\tGOAMD64=%s\n' "${FIXTURE_GOAMD64:-v1}"
		if [ -n "${FIXTURE_GOEXPERIMENT:-}" ]; then
			printf '\tbuild\tGOEXPERIMENT=%s\n' "$FIXTURE_GOEXPERIMENT"
		fi
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
	if [ -n "${FIXTURE_SLEEP_IMPL:-}" ] && [[ "$bench" == *"impl=$FIXTURE_SLEEP_IMPL\$" ]]; then
		sleep 30 &
		sleep_pid=$!
		echo "$sleep_pid" >>"$FIXTURE_DATA/sleep-pids"
		trap 'kill "$sleep_pid"; exit 1' QUIT
		wait "$sleep_pid"
	fi
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
elif [ -n "${FIXTURE_LSCPU_SMT:-}" ]; then
	awk 'BEGIN { for (c = 0; c < 24; c++) printf "%d,%d,0\n", c, int(c / 2) }'
else
	awk -v cores="${FIXTURE_LSCPU_CORES:-8}" \
		'BEGIN { for (c = 0; c < cores; c++) printf "%d,%d,0\n", c, c }'
fi
LSCPU
cat >"$FIXTURE_TOOLS/taskset" <<'TASKSET'
#!/bin/sh
echo "$2" >>"$FIXTURE_DATA/taskset-cpus"
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
# In CSV mode, prints the real benchstat -format csv output saved for the first kind in -col.
# Exits 1 with a message on stderr for an unknown flag or a flag after the file name.
# FIXTURE_BENCHSTAT makes the call that FIXTURE_BENCHSTAT_FORMAT names, text or csv, print an
# error and no output (error), print nothing (empty), or add a cell warning (warning), each with
# exit status 0, as real benchstat does.
cat >"$FIXTURE_TOOLS/benchstat" <<'BENCHSTAT'
#!/bin/sh
csv=false
columns=''
file=''
while [ $# -gt 0 ]; do
	case "$1" in
	-*)
		if [ -n "$file" ]; then
			echo "benchstat: flag $1 after the file name $file" >&2
			exit 1
		fi
		;;
	esac
	case "$1" in
	-format)
		[ "$2" != csv ] || csv=true
		shift
		;;
	-col)
		columns=$2
		shift
		;;
	-row | -filter) shift ;;
	-*)
		echo "benchstat: unknown flag $1" >&2
		exit 1
		;;
	*) file=$1 ;;
	esac
	shift
done
format=text
[ "$csv" = false ] || format=csv
mode=''
if [ "${FIXTURE_BENCHSTAT_FORMAT:-}" = "$format" ]; then
	mode=${FIXTURE_BENCHSTAT:-}
fi
case "$mode" in
error)
	echo "benchstat: open samples.txt: no such file or directory" >&2
	exit 0
	;;
empty) exit 0 ;;
warning) echo 'B3: need >= 6 samples for confidence interval at level 0.95' >&2 ;;
esac
if [ "$csv" = false ]; then
	echo fixture-benchstat
	exit 0
fi
first=${columns#/impl@(}
first=${first%% *}
cat "$FIXTURE_DATA/benchstat-csv/$first.csv"
BENCHSTAT

# Stands in for the Rust programs and the gochan and ring programs. It logs each call, prints one
# result line, takes the seconds of call n from the nth word of FIXTURE_SECONDS, or the last
# word, and misbehaves on the invocation and call that FIXTURE_MISBEHAVE and
# FIXTURE_MISBEHAVE_ROUND name. Each invocation runs once per round, so call n is round n.
cat >"$FIXTURE_TOOLS/crossbeam-program" <<'PROGRAM'
#!/usr/bin/env bash
set -euo pipefail
program=$(basename "$0")
case "$program" in
gochan) kind=Go-chan ;;
ring)
	kind="Go-$1"
	shift
	;;
*) kind="Rust-$program" ;;
esac
workload=$1
calls="$FIXTURE_DATA/crossbeam-calls"
round=$(($(grep -cxF "$kind $workload" "$calls" || true) + 1))
echo "$kind $workload" >>"$calls"
read -r -a seconds <<<"${FIXTURE_SECONDS:-0.5}"
if [ "$round" -le ${#seconds[@]} ]; then
	secs=${seconds[round - 1]}
else
	secs=${seconds[-1]}
fi
lang=${kind%%-*}
impl=${kind#*-}
mode=''
if [ -n "${FIXTURE_MISBEHAVE:-}" ] && [ "$round" -eq "${FIXTURE_MISBEHAVE_ROUND:-1}" ]; then
	IFS=: read -r bad_kind bad_workload bad_mode <<<"$FIXTURE_MISBEHAVE"
	# The kind and workload are glob patterns, so '*:*:exit' makes every invocation fail.
	# shellcheck disable=SC2053
	if [[ "$kind" == $bad_kind ]] && [[ "$workload" == $bad_workload ]]; then
		mode=$bad_mode
	fi
fi
line="$workload $lang $impl $secs sec"
case "$mode" in
'') echo "$line" ;;
exit)
	echo "$line"
	exit 3
	;;
stderr)
	echo "$line"
	echo 'fixture warning' >&2
	;;
two-lines)
	echo "$line"
	echo "$line"
	;;
wrong-name) echo "wrong_workload $lang $impl $secs sec" ;;
wrong-lang) echo "$workload Wrong $impl $secs sec" ;;
wrong-impl) echo "$workload $lang wrong-impl $secs sec" ;;
extra-field) echo "$line extra" ;;
ms) echo "$workload $lang $impl $secs ms" ;;
bad-number) echo "$workload $lang $impl x.y sec" ;;
sleep)
	# The sleep replaces this process, so a KILL to the invocation's pid leaves nothing
	# behind, as with the real single-process programs.
	echo "$$" >"$FIXTURE_DATA/sleeping-pid.tmp"
	mv "$FIXTURE_DATA/sleeping-pid.tmp" "$FIXTURE_DATA/sleeping-pid"
	exec sleep 300
	;;
trap-sleep)
	# Records the signal the watchdog sends first. The trap kills the sleep, so the stub leaves
	# nothing behind.
	sleep 300 &
	sleep_pid=$!
	echo "$sleep_pid" >"$FIXTURE_DATA/trap-sleep-pid"
	trap 'kill "$sleep_pid"; echo QUIT >"$FIXTURE_DATA/trap-signal"; exit 1' QUIT
	trap 'kill "$sleep_pid"; echo TERM >"$FIXTURE_DATA/trap-signal"; exit 1' TERM
	wait "$sleep_pid"
	;;
*)
	echo "unknown FIXTURE_MISBEHAVE mode $mode" >&2
	exit 9
	;;
esac
PROGRAM

# The -v output has one build-script line, which sets no opt-level, and three crate lines with
# exactly the release profile's codegen options. FIXTURE_RUSTC_FLAGS, FIXTURE_OPT_LEVEL,
# FIXTURE_RUSTC_NO_OPT_LEVEL, and FIXTURE_RUSTC_PATH change the last crate line.
# FIXTURE_CARGO_NO_RUNNING leaves out every Running line.
cat >"$FIXTURE_TOOLS/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
-V) echo 'cargo 1.99.1 (fixture)' ;;
fetch | test) : ;;
build)
	target=''
	verbose=false
	while [ $# -gt 0 ]; do
		case "$1" in
		--target-dir)
			target=$2
			shift
			;;
		-v) verbose=true ;;
		esac
		shift
	done
	[ -n "$target" ]
	mkdir -p "$target/release"
	for program in crossbeam-channel arrayqueue arrayqueue-backoff; do
		cp "$FIXTURE_TOOLS/crossbeam-program" "$target/release/$program"
	done
	[ "$verbose" = true ] || exit 0
	rustc="$(rustc --print sysroot)/bin/rustc"
	naming='-C metadata=x -C extra-filename=-x'
	echo '   Compiling crossbeam-utils v0.8.23'
	if [ -n "${FIXTURE_CARGO_NO_RUNNING:-}" ]; then
		echo '    Finished `release` profile [optimized] target(s) in 0.01s'
		exit 0
	fi
	echo "     Running \`$rustc --crate-name build_script_build --crate-type bin" \
		"-C embed-bitcode=no -C debug-assertions=off $naming\`"
	for crate in crossbeam_utils crossbeam_bench arrayqueue; do
		program=$rustc
		opt_level='-C opt-level=3 '
		extra=''
		if [ "$crate" = arrayqueue ]; then
			program=${FIXTURE_RUSTC_PATH:-$rustc}
			opt_level="-C opt-level=${FIXTURE_OPT_LEVEL:-3} "
			if [ -n "${FIXTURE_RUSTC_NO_OPT_LEVEL:-}" ]; then
				opt_level=''
			fi
			extra=${FIXTURE_RUSTC_FLAGS:+ $FIXTURE_RUSTC_FLAGS}
		fi
		echo "     Running \`$program --crate-name $crate --crate-type lib" \
			"${opt_level}-C embed-bitcode=no -C codegen-units=16 $naming" \
			"-C strip=debuginfo$extra\`"
	done
	echo '    Finished `release` profile [optimized] target(s) in 0.01s'
	;;
*)
	echo "unexpected fixture cargo: $*" >&2
	exit 9
	;;
esac
CARGO
cat >"$FIXTURE_TOOLS/rustc" <<'RUSTC'
#!/bin/sh
case "$*" in
-V) echo "${FIXTURE_RUSTC_VERSION:-rustc 1.99.1 (fixture)}" ;;
'--print sysroot') echo "$FIXTURE_DATA/sysroot" ;;
*)
	echo "unexpected fixture rustc: $*" >&2
	exit 9
	;;
esac
RUSTC
cat >"$FIXTURE_TOOLS/uv" <<'UV'
#!/bin/sh
for last in "$@"; do :; done
echo "$*" >>"$FIXTURE_DATA/uv-args"
: >"$(dirname "$last")/plot.png"
UV

# Real benchstat -format csv output, one file per Go kind as the base, for a hand-made
# samples.txt with 6 samples in each of three cells: bounded1024_spsc, where Go-dpdk is faster
# than Rust-arrayqueue; bounded1_mpsc, where Go-chan and Rust-crossbeam-channel do not differ;
# and bounded2p18_mpmc, where Go-frontier is faster than Go-dpdk. benchstat leaves out a base
# with no samples, so the Go-dpdk-mpmc and Go-frontier-mpmc files use Rust-crossbeam-channel.
mkdir "$fixture/benchstat-csv"
cat >"$fixture/benchstat-csv/Go-chan.csv" <<CSV
,Go-chan,,Rust-crossbeam-channel,,,,Rust-arrayqueue,,,,Go-dpdk,,,,Go-frontier,,,
,sec/op,CI,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P
bounded1 mpsc,3.0350000000000004e-08,2%,3.0350000000000004e-08,1%,~,p=0.803 n=6
bounded1024 spsc,,,,,,,2.02e-08,1%,,,1.02e-08,2%
bounded2p18 mpmc,,,,,,,,,,,5.0200000000000006e-08,0%,,,4.02e-08,0%
geomean,3.0349999999999964e-08,,3.0349999999999964e-08,,+0.00%,,2.0200000000000015e-08,,?,,\
2.262830086418326e-08,,?,,4.0200000000000056e-08,,?,
CSV
cat >"$fixture/benchstat-csv/Go-dpdk.csv" <<CSV
,Go-dpdk,,Rust-crossbeam-channel,,,,Rust-arrayqueue,,,,Go-chan,,,,Go-frontier,,,
,sec/op,CI,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P
bounded1 mpsc,,,3.0350000000000004e-08,1%,,,,,,,3.0350000000000004e-08,2%
bounded1024 spsc,1.02e-08,2%,,,,,2.02e-08,1%,+98.04%,p=0.002 n=6
bounded2p18 mpmc,5.0200000000000006e-08,0%,,,,,,,,,,,,,4.02e-08,0%,-19.92%,p=0.002 n=6
geomean,2.262830086418326e-08,,3.0349999999999964e-08,,?,,2.0200000000000015e-08,,+98.04%,,\
3.0349999999999964e-08,,?,,4.0200000000000056e-08,,-19.92%,
CSV
cat >"$fixture/benchstat-csv/Go-frontier.csv" <<CSV
,Go-frontier,,Rust-crossbeam-channel,,,,Rust-arrayqueue,,,,Go-chan,,,,Go-dpdk,,,
,sec/op,CI,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P
bounded1 mpsc,,,3.0350000000000004e-08,1%,,,,,,,3.0350000000000004e-08,2%
bounded1024 spsc,,,,,,,2.02e-08,1%,,,,,,,1.02e-08,2%
bounded2p18 mpmc,4.02e-08,0%,,,,,,,,,,,,,5.0200000000000006e-08,0%,+24.88%,p=0.002 n=6
geomean,4.0200000000000056e-08,,3.0349999999999964e-08,,?,,2.0200000000000015e-08,,?,,\
3.0349999999999964e-08,,?,,2.262830086418326e-08,,+24.88%,
CSV
cat >"$fixture/benchstat-csv/Go-dpdk-mpmc.csv" <<CSV
,Rust-crossbeam-channel,,Rust-arrayqueue,,,,Go-chan,,,,Go-dpdk,,,,Go-frontier,,,
,sec/op,CI,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P
bounded1 mpsc,3.0350000000000004e-08,1%,,,,,3.0350000000000004e-08,2%,~,p=0.803 n=6
bounded1024 spsc,,,2.02e-08,1%,,,,,,,1.02e-08,2%
bounded2p18 mpmc,,,,,,,,,,,5.0200000000000006e-08,0%,,,4.02e-08,0%
geomean,3.0349999999999964e-08,,2.0200000000000015e-08,,?,,3.0349999999999964e-08,,+0.00%,,\
2.262830086418326e-08,,?,,4.0200000000000056e-08,,?,
CSV
cat >"$fixture/benchstat-csv/Go-frontier-mpmc.csv" <<CSV
,Rust-crossbeam-channel,,Rust-arrayqueue,,,,Go-chan,,,,Go-dpdk,,,,Go-frontier,,,
,sec/op,CI,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P,sec/op,CI,vs base,P
bounded1 mpsc,3.0350000000000004e-08,1%,,,,,3.0350000000000004e-08,2%,~,p=0.803 n=6
bounded1024 spsc,,,2.02e-08,1%,,,,,,,1.02e-08,2%
bounded2p18 mpmc,,,,,,,,,,,5.0200000000000006e-08,0%,,,4.02e-08,0%
geomean,3.0349999999999964e-08,,2.0200000000000015e-08,,?,,3.0349999999999964e-08,,+0.00%,,\
2.262830086418326e-08,,?,,4.0200000000000056e-08,,?,
CSV
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
rm -f "$FIXTURE_DATA/sleep-pids"
timeout_start=$SECONDS
run_case invocation-timeout accepted env RING_COMPARE_INVOCATION_LIMIT=1 FIXTURE_SLEEP_IMPL=chan \
	bash "$compare" -rounds 1
timeout_elapsed=$((SECONDS - timeout_start))
[ "$timeout_elapsed" -lt 20 ] || {
	echo "invocation-timeout: took ${timeout_elapsed}s, want under 20s" >&2
	exit 1
}
timeout_results=$(sed -n 's/^results: //p' "$fixture/invocation-timeout.log")
[ -n "$timeout_results" ] || {
	echo "invocation-timeout: the log has no results: line" >&2
	exit 1
}
timeout_state="$timeout_results/state.txt"
grep -q '^status: complete with findings$' "$timeout_state"
timeout_findings=$(awk '/^## findings$/ { p = 1; next } /^## / { p = 0 } p' "$timeout_state")
[ "$(printf '%s\n' "$timeout_findings" | wc -l)" -eq 1 ] || {
	printf 'invocation-timeout: want one finding, got:\n%s\n' "$timeout_findings" >&2
	exit 1
}
[[ "$timeout_findings" == *"chan timed out after 1s"* ]] || {
	echo "invocation-timeout: unexpected finding: $timeout_findings" >&2
	exit 1
}
[ -s "$FIXTURE_DATA/sleep-pids" ] || {
	echo "invocation-timeout: the stub never started its sleep" >&2
	exit 1
}
while read -r sleep_pid; do
	! kill -0 "$sleep_pid" 2>/dev/null || {
		echo "invocation-timeout: sleep $sleep_pid is still running" >&2
		exit 1
	}
done <"$FIXTURE_DATA/sleep-pids"
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

crossbeam="$FIXTURE_ROOT/scripts/crossbeam-bench.sh"

# The wrapper refuses these names, so the suite unsets them to pass in a shell that exports them.
# The preflight cases set their variable explicitly.
refused_names=(GOMAXPROCS GOGC GODEBUG GOMEMLIMIT GOTRACEBACK GLIBC_TUNABLES LD_PRELOAD RUSTFLAGS
	CARGO_BUILD_RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER)
for name in "${refused_names[@]}"; do
	unset "$name"
done
while read -r name; do
	unset "$name"
done < <(compgen -e | grep -E '^(CARGO_PROFILE_|CARGO_BUILD_RUSTC|MALLOC_)' || true)

fail() {
	echo "$*" >&2
	exit 1
}

# Runs a command that execs the wrapper, as env and bash do, so the pid it starts is the
# wrapper's. Sets wrapper_status and wrapper_results, the results directory named for that pid.
run_wrapper() {
	local name=$1
	local pid
	shift
	rm -f "$FIXTURE_DATA/taskset-cpus" "$FIXTURE_DATA/sleeping-pid"
	: >"$FIXTURE_DATA/crossbeam-calls"
	wrapper_status=0
	"$@" >"$fixture/$name.log" 2>&1 &
	pid=$!
	wait "$pid" || wrapper_status=$?
	wrapper_results=$(find "$HOME/crossbeam-compare" -mindepth 1 -maxdepth 1 \
		-name "*-$pid" 2>/dev/null || true)
	printf '%s: exit=%s\n' "$name" "$wrapper_status"
}

# Fails unless state.txt ends with the status and the wrapper exited 0 for complete and 1
# otherwise.
expect_status() {
	local name=$1
	local want="status: $2"
	local want_exit=1
	local last=''

	if [ "$2" = complete ]; then
		want_exit=0
	fi
	if [ -n "$wrapper_results" ]; then
		last=$(tail -n 1 "$wrapper_results/state.txt")
	fi
	[ "$last" = "$want" ] || {
		cat "$fixture/$name.log" >&2
		fail "$name: state.txt ends with '$last', want '$want'"
	}
	[ "$wrapper_status" -eq "$want_exit" ] || {
		cat "$fixture/$name.log" >&2
		fail "$name: the wrapper exited with status $wrapper_status, want $want_exit"
	}
}

# Fails unless the log of the case holds message.
expect_log() {
	local name=$1
	local message=$2

	grep -qF -- "$message" "$fixture/$name.log" || {
		cat "$fixture/$name.log" >&2
		fail "$name: the log lacks '$message'"
	}
}

# Prints the lines under "## <heading>" in state.txt, up to the next heading.
state_section() {
	awk -v heading="## $1" '$0 == heading { p = 1; next } /^## / { p = 0 } p' \
		"$wrapper_results/state.txt"
}

# Prints one "<kind> <workload>" line per invocation of a round, from the design's table.
expected_invocations() {
	local kind
	local topologies
	local topology
	local prefix

	while read -r kind topologies; do
		for topology in ${topologies//,/ }; do
			if [ "$topology" = seq ]; then
				echo "$kind bounded2p23_seq"
				continue
			fi
			for prefix in bounded1 bounded1024 bounded2p18 bounded2p23; do
				echo "$kind ${prefix}_$topology"
			done
		done
	done <<'TABLE'
Rust-crossbeam-channel seq,spsc,mpsc,mpmc
Rust-arrayqueue seq,spsc,mpsc,mpmc
Rust-arrayqueue-backoff seq,spsc,mpsc,mpmc
Go-chan seq,spsc,mpsc,mpmc
Go-dpdk seq,spsc,mpsc,mpmc
Go-frontier mpsc,mpmc
Go-dpdk-mpmc seq,spsc,mpsc
Go-frontier-mpmc seq,spsc,mpsc
TABLE
}

significant_pair='bounded1024 spsc Go-dpdk Rust-arrayqueue Go-dpdk'
go_pair='bounded2p18 mpmc Go-dpdk Go-frontier Go-frontier'

# Seconds and their nanoseconds per message for 5_000_000 messages, one pair per round. The
# values around each median have one and two integer digits, so a lexical sort misorders them.
sample_values=(12:2400.0000 2:400.0000 10:2000.0000 4:800.0000 8:1600.0000 6:1200.0000
	14:2800.0000)
fixture_seconds=$(printf '%s\n' "${sample_values[@]}" | cut -d: -f1 | paste -sd' ' -)

# Prints, sorted, the samples.txt lines that the first rounds of sample_values give every
# invocation, except that skipped_cell, "<kind> <workload>", has no round 1 sample.
expected_samples() {
	local rounds=$1
	local skipped_cell=${2:-}
	local kind
	local workload
	local value
	local round

	expected_invocations | while read -r kind workload; do
		round=0
		for value in $(printf '%s\n' "${sample_values[@]}" | head -n "$rounds"); do
			round=$((round + 1))
			if [ "$round" -eq 1 ] && [ "$kind $workload" = "$skipped_cell" ]; then
				continue
			fi
			printf 'BenchmarkSuite/cap=%s/topo=%s/impl=%s 5000000 %s ns/op\n' \
				"${workload%_*}" "${workload#*_}" "$kind" "${value#*:}"
		done
	done | sort
}

# Prints the median.txt that has median for every cell except other_cell, which has
# other_median.
expected_medians() {
	local median=$1
	local other_cell=${2:-}
	local other_median=${3:-}
	local kind
	local workload
	local value

	expected_invocations | while read -r kind workload; do
		value=$median
		if [ "$kind $workload" = "$other_cell" ]; then
			value=$other_median
		fi
		printf '%-25s %-15s %s sec\n' "$workload" "${kind%%-*} ${kind#*-}" "$value"
	done
}

crossbeam_one_round() {
	local name=crossbeam-one-round
	local log="$FIXTURE_DATA/crossbeam-calls"

	run_wrapper "$name" env FIXTURE_LSCPU_SMT=1 bash "$crossbeam" -rounds 1
	expect_status "$name" "complete with findings"
	diff <(sort "$log") <(expected_invocations | sort) ||
		fail "$name: the calls differ from the 91 invocations, each once"
	[ "$(sort -u "$FIXTURE_DATA/taskset-cpus")" = 0,2,4,6,8,10,12,14 ] ||
		fail "$name: taskset got $(sort -u "$FIXTURE_DATA/taskset-cpus" | paste -sd' ' -)"
	[ "$(wc -l <"$FIXTURE_DATA/taskset-cpus")" -eq 91 ] ||
		fail "$name: want 91 taskset calls"
	# The build checks pass without any FIXTURE_ setting, with a build-script line in the log.
	grep -F -- '--crate-name build_script_build' "$wrapper_results/cargo-build.txt" |
		grep -qv 'opt-level=' || fail "$name: the cargo log has no build-script line"
}

crossbeam_rejections() {
	local mode
	local reason
	local name
	local timeout
	local findings

	for mode in exit stderr two-lines wrong-name wrong-lang wrong-impl extra-field ms bad-number \
		sleep; do
		case "$mode" in
		exit) reason='exited with status 3' ;;
		stderr) reason='wrote to stderr' ;;
		two-lines | wrong-name | wrong-lang | wrong-impl | extra-field | ms | bad-number)
			reason='did not write one result line'
			;;
		sleep) reason='timed out after 1s, sent SIGTERM' ;;
		esac
		timeout=120
		if [ "$mode" = sleep ]; then
			timeout=1
		fi
		name="crossbeam-reject-$mode"
		run_wrapper "$name" env FIXTURE_MISBEHAVE="Rust-arrayqueue:bounded1024_mpsc:$mode" \
			bash "$crossbeam" -rounds 1 -timeout "$timeout"
		expect_status "$name" "complete with findings"
		findings=$(state_section findings)
		if [ "$(printf '%s\n' "$findings" | wc -l)" -ne 1 ] ||
			[[ "$findings" != "round 1 Rust-arrayqueue bounded1024_mpsc $reason"* ]]; then
			fail "$name: want one finding with '$reason', got: $findings"
		fi
		[ "$(wc -l <"$wrapper_results/samples.txt")" -eq 90 ] ||
			fail "$name: want 90 samples"
		! grep -q '^BenchmarkSuite/cap=bounded1024/topo=mpsc/impl=Rust-arrayqueue ' \
			"$wrapper_results/samples.txt" || fail "$name: the rejected sample was kept"
	done
}

# A Go program gets SIGQUIT first when it runs past the time limit.
crossbeam_go_timeout() {
	local name=crossbeam-go-timeout
	local findings

	rm -f "$FIXTURE_DATA/trap-signal" "$FIXTURE_DATA/trap-sleep-pid"
	# Job control keeps the wrapper from starting with QUIT ignored, as a background job
	# otherwise would, and bash cannot trap a signal ignored when it started.
	set -m
	run_wrapper "$name" env FIXTURE_MISBEHAVE=Go-dpdk:bounded1_spsc:trap-sleep \
		bash "$crossbeam" -rounds 1 -timeout 1
	set +m
	if [ -s "$FIXTURE_DATA/trap-sleep-pid" ]; then
		case_pids+=("$(cat "$FIXTURE_DATA/trap-sleep-pid")")
	fi
	expect_status "$name" "complete with findings"
	findings=$(state_section findings)
	if [ "$(printf '%s\n' "$findings" | wc -l)" -ne 1 ] ||
		[[ "$findings" != *"timed out after 1s, sent SIGQUIT"* ]]; then
		fail "$name: want one finding with 'timed out after 1s, sent SIGQUIT', got: $findings"
	fi
	[ "$(cat "$FIXTURE_DATA/trap-signal" 2>/dev/null)" = QUIT ] ||
		fail "$name: the stub did not get SIGQUIT"
	case_pids=()
}

# Each benchstat failure that exits 0 becomes a finding naming the call's stderr file, and a cell
# warning does not.
crossbeam_benchstat_failures() {
	local format
	local mode
	local name
	local kind
	local finding
	local findings

	for format in text csv; do
		for mode in error empty; do
			name="crossbeam-benchstat-$format-$mode"
			run_wrapper "$name" env FIXTURE_BENCHSTAT="$mode" FIXTURE_BENCHSTAT_FORMAT="$format" \
				bash "$crossbeam" -rounds 1
			expect_status "$name" "complete with findings"
			findings=$(state_section findings)
			for kind in Go-chan Go-dpdk Go-frontier Go-dpdk-mpmc Go-frontier-mpmc; do
				finding="benchstat failed for summary-$kind.txt,\
 see $wrapper_results/benchstat-$kind-text.err"
				if [ "$format" = csv ]; then
					finding="benchstat -format csv failed for $kind,\
 see $wrapper_results/benchstat-$kind.err"
				fi
				grep -qxF -- "$finding" <<<"$findings" ||
					fail "$name: want the finding '$finding', got: $findings"
			done
		done
	done
	name="crossbeam-benchstat-csv-warning"
	run_wrapper "$name" env FIXTURE_BENCHSTAT=warning FIXTURE_BENCHSTAT_FORMAT=csv \
		bash "$crossbeam" -rounds 1
	expect_status "$name" "complete with findings"
	findings=$(state_section findings)
	! grep -q benchstat <<<"$findings" || fail "$name: want no benchstat finding, got: $findings"
}

crossbeam_all_fail() {
	local name=crossbeam-all-fail

	run_wrapper "$name" env FIXTURE_MISBEHAVE='*:*:exit' bash "$crossbeam" -rounds 1
	if [ "$wrapper_status" -eq 0 ] || ! grep -q 'no invocation succeeded' "$fixture/$name.log"
	then
		cat "$fixture/$name.log" >&2
		fail "$name: want a stop with 'no invocation succeeded'"
	fi
	expect_status "$name" "aborted at the summary"
	[ "$(state_section findings | wc -l)" -eq 91 ] || fail "$name: want 91 findings"
}

crossbeam_complete() {
	local name=crossbeam-complete
	local file
	local log="$FIXTURE_DATA/crossbeam-calls"
	local round_1="$fixture/$name-round-1.txt"
	local round_2="$fixture/$name-round-2.txt"

	run_wrapper "$name" env FIXTURE_SECONDS="$fixture_seconds" bash "$crossbeam" -seed 5 \
		-rounds 6
	expect_status "$name" complete
	grep -qx 'seed: 5' "$wrapper_results/state.txt" || fail "$name: state.txt lacks 'seed: 5'"
	for file in samples.txt summary-Go-chan.txt summary-Go-dpdk.txt summary-Go-frontier.txt \
		summary-Go-dpdk-mpmc.txt summary-Go-frontier-mpmc.txt significant.txt median.txt; do
		[ -s "$wrapper_results/$file" ] || fail "$name: $file is missing or empty"
	done
	diff "$wrapper_results/significant.txt" <(printf '%s\n' "$significant_pair" "$go_pair") ||
		fail "$name: significant.txt differs"
	diff <(sort "$wrapper_results/samples.txt") <(expected_samples 6) ||
		fail "$name: samples.txt differs"
	diff "$wrapper_results/median.txt" <(expected_medians 7.000000) ||
		fail "$name: median.txt differs"
	[ -e "$wrapper_results/plot.png" ] || fail "$name: plot.png is missing next to median.txt"
	grep -qF -- "$wrapper_results/median.txt" "$FIXTURE_DATA/uv-args" ||
		fail "$name: the uv arguments do not name $wrapper_results/median.txt"
	sed -n '1,91p' "$log" >"$round_1"
	sed -n '92,182p' "$log" >"$round_2"
	! cmp -s "$round_1" <(expected_invocations) || fail "$name: round 1 runs in table order"
	! cmp -s "$round_1" "$round_2" || fail "$name: rounds 1 and 2 run in the same order"

	# Round r uses seed + r, so round 2 of seed 5 runs in the order of round 1 of seed 6.
	name="crossbeam-seed"
	run_wrapper "$name" bash "$crossbeam" -seed 6 -rounds 1
	expect_status "$name" "complete with findings"
	grep -qx 'seed: 6' "$wrapper_results/state.txt" || fail "$name: state.txt lacks 'seed: 6'"
	cmp -s "$round_2" "$log" ||
		fail "$name: round 1 of seed 6 differs from round 2 of seed 5"
}

crossbeam_preflight_environment() {
	local setting
	local name
	local settings=()

	for name in "${refused_names[@]}"; do
		settings+=("$name=")
	done
	# One name for each refused prefix.
	settings+=(CARGO_PROFILE_RELEASE_LTO=true CARGO_BUILD_RUSTC=x MALLOC_ARENA_MAX=1)
	for setting in "${settings[@]}"; do
		name="crossbeam-environment-${setting%%=*}"
		run_wrapper "$name" env "$setting" bash "$crossbeam" -rounds 1
		if [ "$wrapper_status" -eq 0 ] || [ -n "$wrapper_results" ] ||
			! grep 'before timing' "$fixture/$name.log" | grep -qF "${setting%%=*}"; then
			cat "$fixture/$name.log" >&2
			fail "$name: want a stop in preflight"
		fi
	done
}

# Fails unless the case stopped before making its results directory, with message in its log.
expect_preflight_stop() {
	local name=$1
	local message=$2

	if [ "$wrapper_status" -eq 0 ] || [ -n "$wrapper_results" ]; then
		cat "$fixture/$name.log" >&2
		fail "$name: want a stop in preflight"
	fi
	expect_log "$name" "$message"
}

crossbeam_preflight_checks() {
	local name
	local no_cargo_tools="$fixture/tools-no-cargo"
	local lib="$FIXTURE_ROOT/benchmark/crossbeam/src/lib.rs"
	local original_lib

	name="crossbeam-preflight-rustc-version"
	run_wrapper "$name" env FIXTURE_RUSTC_VERSION='rustc 1.98.0 (fixture)' \
		bash "$crossbeam" -rounds 1
	expect_preflight_stop "$name" "prints 'rustc 1.98.0 (fixture)', want 1.99.1"

	name="crossbeam-preflight-no-cargo"
	cp -R "$FIXTURE_TOOLS" "$no_cargo_tools"
	rm "$no_cargo_tools/cargo"
	run_wrapper "$name" env PATH="$no_cargo_tools:/usr/bin:/bin" bash "$crossbeam" -rounds 1
	expect_preflight_stop "$name" "missing tools: cargo"

	name="crossbeam-preflight-cores"
	run_wrapper "$name" env FIXTURE_LSCPU_CORES=7 bash "$crossbeam" -rounds 1
	expect_preflight_stop "$name" "the one-thread-per-core set has 7 CPUs, need at least 8"

	# The wrapper compares the message counts after the build, so this case stops there.
	name="crossbeam-preflight-messages"
	original_lib=$(cat "$lib")
	printf 'pub const MESSAGES: usize = 4_000_000;\n' >"$lib"
	run_wrapper "$name" bash "$crossbeam" -rounds 1
	printf '%s\n' "$original_lib" >"$lib"
	[ "$wrapper_status" -ne 0 ] || fail "$name: want a stop"
	expect_status "$name" "aborted at the build"
	expect_log "$name" "is '4000000', workloads.Messages is 5000000"
	[ ! -s "$FIXTURE_DATA/crossbeam-calls" ] || fail "$name: an invocation ran"
}

crossbeam_real_benchstat() {
	local name=crossbeam-real-benchstat
	local real_benchstat
	local real_tools="$fixture/tools-real-benchstat"
	local summary
	local want_columns
	local want_rows
	local prefix
	local topology
	local invocations
	local findings
	local file

	real_benchstat=$(PATH=$FIXTURE_ORIGINAL_PATH command -v benchstat || true)
	if [ -z "$real_benchstat" ]; then
		echo "$name: skipped, benchstat is not on PATH"
		return
	fi
	cp -R "$FIXTURE_TOOLS" "$real_tools"
	ln -sf "$real_benchstat" "$real_tools/benchstat"
	# The rejected sample leaves the Go-dpdk bounded1_spsc cell empty.
	run_wrapper "$name" env PATH="$real_tools:$FIXTURE_ORIGINAL_PATH" \
		FIXTURE_MISBEHAVE=Go-dpdk:bounded1_spsc:exit bash "$crossbeam" -rounds 1
	expect_status "$name" "complete with findings"
	findings=$(state_section findings)
	if [ "$(printf '%s\n' "$findings" | wc -l)" -ne 1 ] ||
		[[ "$findings" != "round 1 Go-dpdk bounded1_spsc exited with status 3"* ]]; then
		fail "$name: want only the Go-dpdk rejection, got: $findings"
	fi
	for file in "$wrapper_results"/summary-*.txt; do
		[ -s "$file" ] || fail "$name: $file is missing or empty"
	done
	# Per-cell warnings such as "B3: need >= 6 samples ..." are normal.
	for file in "$wrapper_results"/benchstat-*.err; do
		! grep -vE '^[A-Z]+[0-9]+: ' "$file" >&2 ||
			fail "$name: $file has a line that is not a cell warning"
	done
	summary="$wrapper_results/summary-Go-dpdk.txt"
	want_columns=(Go-dpdk Rust-crossbeam-channel Rust-arrayqueue Rust-arrayqueue-backoff Go-chan
		Go-frontier Go-dpdk-mpmc Go-frontier-mpmc)
	diff <(head -n 1 "$summary" | grep -oE '(Go|Rust)-[a-z-]+') \
		<(printf '%s\n' "${want_columns[@]}") || fail "$name: the columns are out of order"
	invocations=$(expected_invocations)
	want_rows=$(for prefix in bounded1 bounded1024 bounded2p18 bounded2p23; do
		for topology in seq spsc mpsc mpmc; do
			if grep -q " ${prefix}_$topology\$" <<<"$invocations"; then
				echo "$prefix $topology"
			fi
		done
	done)
	diff <(awk '/^bounded/ { print $1, $2 }' "$summary") <(printf '%s\n' "$want_rows") ||
		fail "$name: the rows are out of order"
}

crossbeam_excluded_cell() {
	local rounds
	local name
	local cell
	local want_incomplete
	local want_excluded

	for rounds in 7 6; do
		# Go-dpdk is the base column of the Go-dpdk summary, and Rust-arrayqueue is not.
		cell='Go-dpdk bounded1024_spsc'
		want_incomplete=none
		want_excluded=$cell
		if [ "$rounds" -eq 6 ]; then
			cell='Rust-arrayqueue bounded1024_spsc'
			want_incomplete=$cell
			want_excluded=none
		fi
		name="crossbeam-excluded-$rounds"
		run_wrapper "$name" env FIXTURE_SECONDS="$fixture_seconds" \
			FIXTURE_MISBEHAVE="${cell/ /:}:exit" bash "$crossbeam" -rounds "$rounds"
		expect_status "$name" "complete with findings"
		if [ "$(state_section 'incomplete cells')" != "$want_incomplete" ] ||
			[ "$(state_section 'excluded cells')" != "$want_excluded" ]; then
			fail "$name: want incomplete '$want_incomplete' and excluded '$want_excluded'"
		fi
		diff "$wrapper_results/significant.txt" <(echo "$go_pair") ||
			fail "$name: significant.txt differs"
		if [ "$rounds" -eq 7 ]; then
			diff <(sort "$wrapper_results/samples.txt") <(expected_samples 7 "$cell") ||
				fail "$name: samples.txt differs"
			# The excluded cell keeps rounds 2 to 7, whose median is 7.
			diff "$wrapper_results/median.txt" <(expected_medians 8.000000 "$cell" 7.000000) ||
				fail "$name: median.txt differs"
		fi
	done
}

# Succeeds once pid has exited, including as a zombie its parent has not waited for yet.
process_exited() {
	local state

	state=$(ps -o stat= -p "$1" || true)
	[ -z "$state" ] || [[ "$state" == Z* ]]
}

# Runs a command every 0.1 s until it succeeds, for up to the given seconds.
wait_for() {
	local seconds=$1
	local tries
	shift

	for ((tries = 0; tries < seconds * 10; tries++)); do
		if "$@"; then
			return 0
		fi
		sleep 0.1
	done
	"$@"
}

# Prints the pid of the watchdog's sleep, the grandchild of the wrapper that runs "sleep <limit>".
watchdog_sleep_pid() {
	local wrapper_pid=$1
	local limit=$2
	local child
	local grandchild

	for child in $(pgrep -P "$wrapper_pid"); do
		for grandchild in $(pgrep -P "$child"); do
			if [ "$(ps -o args= -p "$grandchild")" = "sleep $limit" ]; then
				echo "$grandchild"
				return 0
			fi
		done
	done
	return 1
}

crossbeam_signals() {
	local signal
	local name
	local pid
	local stub_pid
	local sleep_pid
	local results

	for signal in TERM INT HUP; do
		name="crossbeam-signal-$signal"
		rm -f "$FIXTURE_DATA/sleeping-pid"
		: >"$FIXTURE_DATA/crossbeam-calls"
		# Job control keeps the wrapper from starting with INT ignored, as a background job
		# otherwise would.
		set -m
		env FIXTURE_MISBEHAVE=Go-dpdk:bounded1_spsc:sleep bash "$crossbeam" -rounds 1 \
			-timeout 97 >"$fixture/$name.log" 2>&1 &
		pid=$!
		set +m
		case_pids+=("$pid")
		wait_for 60 test -s "$FIXTURE_DATA/sleeping-pid" ||
			fail "$name: the sleeping stub never started"
		stub_pid=$(cat "$FIXTURE_DATA/sleeping-pid")
		case_pids+=("$stub_pid")
		sleep_pid=$(wait_for 5 watchdog_sleep_pid "$pid" 97) ||
			fail "$name: found no watchdog sleep under the wrapper"
		case_pids+=("$sleep_pid")
		kill "-$signal" "$pid"
		wait_for 5 process_exited "$pid" ||
			fail "$name: the wrapper is running 5 s after SIG$signal"
		wait "$pid" || true
		results=$(find "$HOME/crossbeam-compare" -mindepth 1 -maxdepth 1 -name "*-$pid")
		[ "$(tail -n 1 "$results/state.txt")" = 'status: interrupted' ] ||
			fail "$name: state.txt does not end with 'status: interrupted'"
		! kill -0 "$stub_pid" 2>/dev/null || fail "$name: the stub $stub_pid is still running"
		process_exited "$sleep_pid" || fail "$name: the watchdog sleep $sleep_pid is still running"
		case_pids=()
		echo "$name: interrupted"
	done
}

crossbeam_build_checks() {
	local setting
	local problem
	local name
	local index=0

	# Each line is a setting and the problem the wrapper must report for it.
	while IFS='|' read -r -u 3 setting problem; do
		index=$((index + 1))
		name="crossbeam-build-check-$index"
		run_wrapper "$name" env "$setting" bash "$crossbeam" -rounds 1
		[ "$wrapper_status" -ne 0 ] || fail "$name: want a stop"
		expect_status "$name" "aborted at the build checks"
		expect_log "$name" "$problem"
	done 3<<'CASES'
FIXTURE_RUSTC_FLAGS=-C target-cpu=native|unexpected codegen option target-cpu=native
FIXTURE_RUSTC_FLAGS=-C target-feature=+avx2|unexpected codegen option target-feature=+avx2
FIXTURE_OPT_LEVEL=2|unexpected codegen option opt-level=2
FIXTURE_GOEXPERIMENT=x|was built with GOEXPERIMENT=x
FIXTURE_GOAMD64=v3|was built with GOAMD64=v3
FIXTURE_RUSTC_FLAGS=-C lto=fat|unexpected codegen option lto=fat
FIXTURE_RUSTC_PATH=/usr/bin/sccache|runs /usr/bin/sccache, want
FIXTURE_RUSTC_FLAGS=-O|sets -O
FIXTURE_RUSTC_FLAGS=-C opt-level=3|repeats codegen option opt-level=3
FIXTURE_RUSTC_FLAGS=-Copt-level=2|unexpected codegen option opt-level=2
FIXTURE_RUSTC_FLAGS=--codegen=opt-level=2|unexpected codegen option opt-level=2
FIXTURE_RUSTC_NO_OPT_LEVEL=1|lacks codegen option opt-level=3
FIXTURE_CARGO_NO_RUNNING=1|no crate rustc command line
CASES
}

crossbeam_one_round
crossbeam_rejections
crossbeam_go_timeout
crossbeam_benchstat_failures
crossbeam_all_fail
crossbeam_complete
crossbeam_preflight_environment
crossbeam_preflight_checks
crossbeam_real_benchstat
crossbeam_excluded_cell
crossbeam_signals
crossbeam_build_checks
echo 'runner fixtures complete'
