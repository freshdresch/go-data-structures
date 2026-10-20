#!/usr/bin/env bash
#
# Runs a quick, directional before-and-after comparison of the ring benchmarks for two committed
# refs. See scripts/README.md.

set -euo pipefail

usage() {
	echo "usage: scripts/ring-quick-bench.sh [-rounds N] [-benchtime D] <before-ref> <after-ref>"
	echo "       scripts/ring-quick-bench.sh [-rounds N] [-benchtime D] -reuse <dir>"
}

die() {
	echo "ring-quick-bench: $*" >&2
	exit 1
}

warn() {
	echo "ring-quick-bench: warning: $*" >&2
}

usage_error() {
	echo "ring-quick-bench: $*" >&2
	usage >&2
	exit 1
}

resolve_ref() {
	git -C "$repo" rev-parse --verify --quiet "$1^{commit}" || die "cannot resolve ref '$1'"
}

# Prints the named "## " section of a state.txt, heading included.
state_section() {
	awk -v heading="## $2" '
		$0 == heading { printing = 1; print; next }
		/^## / { printing = 0 }
		printing
	' "$1"
}

# Prints "label: value" from a command, or "label: unknown" with a warning when it fails.
record() {
	local label=$1
	local value
	shift
	if value=$("$@" 2>/dev/null) && [ -n "$value" ]; then
		printf '%s\n' "$value" | awk -v label="$label" '{ print label ": " $0 }'
	else
		warn "could not read $label"
		echo "$label: unknown"
	fi
}

load_average() {
	if [ "$platform" = Darwin ]; then
		sysctl -n vm.loadavg
	else
		cat /proc/loadavg
	fi
}

rounds=10
benchtime=200ms
reuse_dir=""
refs=()

while [ $# -gt 0 ]; do
	case "$1" in
	-h)
		usage
		exit 0
		;;
	-rounds | -benchtime | -reuse)
		[ $# -ge 2 ] || usage_error "$1 needs a value"
		case "$1" in
		-rounds) rounds=$2 ;;
		-benchtime) benchtime=$2 ;;
		-reuse) reuse_dir=$2 ;;
		esac
		shift 2
		;;
	-*)
		usage_error "unknown flag $1"
		;;
	*)
		refs+=("$1")
		shift
		;;
	esac
done

case "$rounds" in
'' | *[!0-9]*) usage_error "-rounds must be a positive integer, got '$rounds'" ;;
esac
# Forces base 10, so a leading zero such as 08 is not read as octal.
rounds=$((10#$rounds))
[ "$rounds" -gt 0 ] || usage_error "-rounds must be a positive integer, got '$rounds'"

if [ -n "$reuse_dir" ]; then
	[ ${#refs[@]} -eq 0 ] || usage_error "-reuse takes no refs"
else
	[ ${#refs[@]} -eq 2 ] || usage_error "need exactly two refs, <before-ref> <after-ref>"
fi

platform=$(uname -s)
case "$platform" in
Darwin) tools="git go benchstat" ;;
Linux) tools="git go benchstat lscpu taskset" ;;
*) die "unsupported platform $platform" ;;
esac

missing=""
for tool in $tools; do
	command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
done
[ -z "$missing" ] || die "missing tools:$missing"

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)

# Keeps the first CPU listed for each (socket, core) pair, so each core gets one thread.
one_cpu_per_core() {
	lscpu -p=CPU,CORE,SOCKET |
		awk -F, '!/^#/ && !seen[$3 "," $2]++ { printf "%s%s", sep, $1; sep = "," }'
}

pin=()
own_cpus=""
if [ "$platform" = Darwin ]; then
	own=10
	ncpu=$(sysctl -n hw.ncpu)
	[ "$ncpu" -ge "$own" ] || die "need at least $own CPUs, hw.ncpu is $ncpu"
else
	own_cpus=$(one_cpu_per_core) || die "could not read the core map from lscpu"
	[ -n "$own_cpus" ] || die "could not derive the one-thread-per-core CPU set"
	own=$(printf '%s\n' "$own_cpus" | tr ',' '\n' | wc -l | tr -d ' ')
	[ "$own" -ge 2 ] ||
		die "the one-thread-per-core set has only $own CPU, need at least 2," \
			"the lscpu core map may be incomplete"
	pin=(taskset -c "$own_cpus")
fi

if [ -z "$reuse_dir" ]; then
	before_hash=$(resolve_ref "${refs[0]}")
	after_hash=$(resolve_ref "${refs[1]}")
	harness_diff=$(git -C "$repo" diff --stat "$before_hash" "$after_hash" -- \
		'ring/alloc_test.go' 'ring/bench_test.go' 'ring/bench_*_test.go' 'ring/race_*_test.go')
	[ -n "$harness_diff" ] || harness_diff=none
else
	for name in before.test after.test state.txt; do
		[ -f "$reuse_dir/$name" ] || die "-reuse directory $reuse_dir has no $name"
	done
	harness_diff=$(state_section "$reuse_dir/state.txt" "harness diff" | sed 1d)
fi

mkdir -p "$HOME/ring-quick"
results="$HOME/ring-quick/$(date +%Y%m%d-%H%M%S)-$$"
mkdir "$results"
state="$results/state.txt"

status="aborted before round 1"
build_dir=""

on_exit() {
	echo "status: $status" >>"$state"
	if [ -n "$build_dir" ]; then
		rm -rf "$build_dir"
	fi
}
trap on_exit EXIT

load_before=$(record "load average" load_average)

build_side() {
	local side=$1
	local hash=$2
	local build_err

	build_dir=$(mktemp -d)
	git -C "$repo" archive "$hash" | tar -x -C "$build_dir"
	if ! build_err=$(
		cd "$build_dir" &&
			GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly \
				go test -c -o "$results/$side.test" ./ring 2>&1 >/dev/null
	); then
		echo "$build_err" >&2
		case "$build_err" in
		*GOTOOLCHAIN=local*)
			die "the $side ref needs a newer Go than the local toolchain"
			;;
		*"cannot find module"* | *"dial tcp"* | *"missing go.sum entry"*)
			die "building $side failed, the build needs the module cache to hold" \
				"the dependencies, or network access"
			;;
		*)
			die "building $side ($hash) failed, see the output above"
			;;
		esac
	fi
	rm -rf "$build_dir"
	build_dir=""
}

if [ -z "$reuse_dir" ]; then
	build_side before "$before_hash"
	build_side after "$after_hash"
else
	cp "$reuse_dir/before.test" "$reuse_dir/after.test" "$results/"
fi

single_flags=(
	-test.run '^$'
	-test.benchmem
	-test.count 1
	-test.benchtime "$benchtime"
	-test.cpu 1
	-test.bench '^BenchmarkRingAPI$/^api=seq$/^elem=int64$'
)
own_pattern='^BenchmarkThroughput$/^mode=(SPSC|MPMC)$/^batch=(1|32)$/^elem=int64$/^impl=ring$'
own_pattern="$own_pattern|^BenchmarkHandoff\$/^impl=ring\$"
ring_api_pattern='^BenchmarkRingAPI$/^mode=(SPSC|MPMC)$/^api=(burst|bulk)$'
ring_api_pattern="$ring_api_pattern/^batch=(1|8|32|64)$/^elem=int64$"
own_pattern="$own_pattern|$ring_api_pattern"
# Go reports no leaf benchmark when the pattern has more levels than its name.
# So api=single gets its own alternative.
single_api_pattern='^BenchmarkRingAPI$/^mode=(SPSC|MPMC)$/^api=single$/^elem=int64$'
own_pattern="$own_pattern|$single_api_pattern"
own_flags=(
	-test.run '^$'
	-test.benchmem
	-test.count 1
	-test.benchtime "$benchtime"
	-test.cpu "$own"
	-test.bench "$own_pattern"
)

single_rows="BenchmarkRingAPI/api=seq/elem=int64"
own_rows="BenchmarkThroughput/mode=SPSC/batch=1/elem=int64/impl=ring-$own
BenchmarkThroughput/mode=SPSC/batch=32/elem=int64/impl=ring-$own
BenchmarkThroughput/mode=MPMC/batch=1/elem=int64/impl=ring-$own
BenchmarkThroughput/mode=MPMC/batch=32/elem=int64/impl=ring-$own
BenchmarkHandoff/impl=ring-$own"
for mode in SPSC MPMC; do
	own_rows="$own_rows
BenchmarkRingAPI/mode=$mode/api=single/elem=int64-$own"
	for api in burst bulk; do
		for batch in 1 8 32 64; do
			own_rows="$own_rows
BenchmarkRingAPI/mode=$mode/api=$api/batch=$batch/elem=int64-$own"
		done
	done
done

command_line() {
	if [ ${#pin[@]} -gt 0 ]; then
		printf '%q ' "${pin[@]}"
	fi
	printf '%s ' "$results/<side>.test"
	printf '%q ' "$@"
	echo
}

binary_info() {
	go version -m "$1" | awk -F'\t' '
		NR == 1 { sub(/^[^:]*: /, ""); print "toolchain=" $0 }
		$2 == "build" && $3 ~ /^(GOOS|GOARCH|GOARM64|GOAMD64|CGO_ENABLED)=/ { print $3 }
	'
}

cpu_model() {
	if [ "$platform" = Darwin ]; then
		sysctl -n machdep.cpu.brand_string
	else
		lscpu | grep '^Model name'
	fi
}

benchstat_module() {
	go version -m "$(command -v benchstat)" | awk '$1 == "mod"'
}

write_state() {
	local scripts_status
	local side
	local name

	echo "## script"
	git -C "$repo" rev-parse HEAD
	scripts_status=$(git -C "$repo" status --porcelain -- scripts/)
	echo "${scripts_status:-clean}"

	if [ -z "$reuse_dir" ]; then
		echo "## refs"
		echo "before: ${refs[0]} $before_hash"
		echo "after: ${refs[1]} $after_hash"
		echo "## harness diff"
		echo "$harness_diff"
	else
		state_section "$reuse_dir/state.txt" refs
		echo "reused: $reuse_dir"
		state_section "$reuse_dir/state.txt" "harness diff"
	fi

	echo "## commands"
	echo "cwd: $results"
	echo "single-CPU: $(command_line "${single_flags[@]}")"
	echo "own-CPU: $(command_line "${own_flags[@]}")"
	echo "rounds: $rounds"
	echo "benchtime: $benchtime"

	echo "## binaries"
	for side in before after; do
		record "$side" binary_info "$results/$side.test"
	done

	echo "## environment"
	local any_set=no
	for name in GOGC GODEBUG GOMEMLIMIT GOMAXPROCS; do
		if [ -n "${!name+x}" ]; then
			warn "$name is set to '${!name}'"
			echo "$name=${!name}"
			any_set=yes
		fi
	done
	[ "$any_set" = yes ] || echo "none set"

	echo "## machine"
	record "cpu model" cpu_model
	record uname uname -a
	echo "$load_before"
	if [ "$platform" = Linux ]; then
		record nproc nproc
		echo "own CPUs: $own_cpus"
	fi

	echo "## benchstat"
	record benchstat benchstat_module
}

write_state >>"$state"
mkdir "$results/rounds"

check_rows() {
	local out=$1
	local expected=$2
	local got
	local missing_rows
	local extra_rows

	# Result rows have the iteration count as their second field, unlike headers or log lines.
	got=$(awk '$1 ~ /^Benchmark/ && $2 ~ /^[0-9]+$/ { print $1 }' "$out" | sort)
	expected=$(printf '%s\n' "$expected" | sort)
	missing_rows=$(comm -23 <(echo "$expected") <(echo "$got"))
	extra_rows=$(comm -13 <(echo "$expected") <(echo "$got"))
	if [ -n "$missing_rows" ] || [ -n "$extra_rows" ]; then
		die "unexpected rows in $out" \
			$'\nmissing:\n'"${missing_rows:-none}"$'\nextra:\n'"${extra_rows:-none}"
	fi
}

run_invocation() {
	local side=$1
	local round=$2
	local kind=$3
	local out="$results/rounds/$round-$side-$kind.txt"
	local exit_status=0
	local flags
	local expected

	if [ "$kind" = single ]; then
		flags=("${single_flags[@]}")
		expected=$single_rows
	else
		flags=("${own_flags[@]}")
		expected=$own_rows
	fi

	# Runs from the results directory, so anything the binary writes stays with the results.
	(cd "$results" && ${pin[@]+"${pin[@]}"} "$results/$side.test" "${flags[@]}") \
		>"$out" 2>&1 || exit_status=$?
	if [ "$exit_status" -ne 0 ] || grep -q -- '--- FAIL' "$out"; then
		tail -n 20 "$out" >&2
		die "$side $kind invocation failed in round $round, exit status $exit_status"
	fi
	check_rows "$out" "$expected"
	cat "$out" >>"$results/$side.txt"
}

run_side() {
	run_invocation "$1" "$2" single
	run_invocation "$1" "$2" own
}

start=$(date +%s)
for ((k = 1; k <= rounds; k++)); do
	status="aborted at round $k"
	echo "round $k/$rounds" >&2
	if ((k % 2 == 1)); then
		run_side before "$k"
		run_side after "$k"
	else
		run_side after "$k"
		run_side before "$k"
	fi
done
end=$(date +%s)
elapsed=$((end - start))
printf '## timing\nelapsed: %ss\n' "$elapsed" >>"$state"

metrics='.unit:(sec/op OR B/s OR allocs/op OR handoff-p50 OR handoff-p90 OR handoff-p99)'
status="rounds complete, comparison failed"
(cd "$results" && benchstat -filter "$metrics" before.txt after.txt) >"$results/comparison.txt"
cat "$results/comparison.txt"

echo
if [ "$harness_diff" != none ]; then
	echo "The benchmark files differ between the refs, so harness changes are part of the" \
		"comparison. The diff stat follows."
	echo "$harness_diff"
fi
echo "The MPMC batch-1 row is high-variance on both platforms, so its interval is usually wide."
echo "The handoff-p50, handoff-p90, and handoff-p99 values are in nanoseconds."
echo "benchstat does not correct p-values across rows and metrics, so a few rows will show a" \
	"significant change by chance."
echo "Results at $benchtime with -test.count 1 per round are not comparable with the full" \
	"sweep's or the decision runs' tables."
echo "results: $results"
echo "elapsed: ${elapsed}s"
status=complete
