#!/usr/bin/env bash
#
# Compares the ring with other Go queues through the benchmark module's BenchmarkCompare, in one
# binary per machine. See scripts/README.md and benchmark/README.md.

set -euo pipefail

prog=ring-compare-bench

# Seconds each round invocation may run. Go's -test.timeout does not cover benchmarks, so the
# script times each invocation itself.
invocation_limit=${RING_COMPARE_INVOCATION_LIMIT:-120}

usage() {
	echo "usage: scripts/ring-compare-bench.sh [-rounds N] [-benchtime D] [-pad N] [-seed N]"
	echo
	echo "  -rounds N     rounds to run, default 10"
	echo "  -benchtime D  benchtime for each benchmark, default 200ms"
	echo "  -pad N        rebuild with N padding pointers, Mac only; without it the Mac pads"
	echo "                with 4 only when runtime.sched lands at offset 96"
	echo "  -seed N       shuffle seed, default the script's pid; a seed repeats an order only"
	echo "                on the same platform, because awk's srand differs between them"
}

usage_error() {
	echo "$prog: $*" >&2
	usage >&2
	exit 1
}

check_positive_integer() {
	local flag=$1
	local value=$2

	case "$value" in
	'' | *[!0-9]*) usage_error "$flag must be a positive integer, got '$value'" ;;
	esac
	# Bash arithmetic wraps on 20 or more digits.
	[ ${#value} -le 9 ] || usage_error "$flag must be a positive integer, got '$value'"
	# Forces base 10, so a leading zero such as 08 is not read as octal.
	[ $((10#$value)) -gt 0 ] || usage_error "$flag must be a positive integer, got '$value'"
}

rounds=10
benchtime=200ms
pad=""
seed=$$

while [ $# -gt 0 ]; do
	case "$1" in
	-h)
		usage
		exit 0
		;;
	-rounds | -benchtime | -pad | -seed)
		[ $# -ge 2 ] || usage_error "$1 needs a value"
		case "$1" in
		-rounds) rounds=$2 ;;
		-benchtime) benchtime=$2 ;;
		-pad) pad=$2 ;;
		-seed) seed=$2 ;;
		esac
		shift 2
		;;
	*)
		usage_error "unknown argument $1"
		;;
	esac
done

check_positive_integer -rounds "$rounds"
rounds=$((10#$rounds))
check_positive_integer -seed "$seed"
seed=$((10#$seed))
if [ -n "$pad" ]; then
	check_positive_integer -pad "$pad"
	pad=$((10#$pad))
fi
benchtime_pattern='^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$|^[0-9]+x$'
if ! [[ $benchtime =~ $benchtime_pattern ]]; then
	usage_error "-benchtime must be a duration such as 200ms or a count such as 100x," \
		"got '$benchtime'"
fi

platform=$(uname -s)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if [ -n "$pad" ] && [ "$platform" != Darwin ]; then
	die "-pad is for the Mac only, this platform never pads"
fi

check_tools
select_cpus

check_ac_power

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)

mkdir -p "$HOME/ring-compare"
results="$HOME/ring-compare/$(date +%Y%m%d-%H%M%S)-$$"
mkdir "$results"
state="$results/state.txt"

status="aborted at the build"

on_exit() {
	echo "status: $status" >>"$state"
}
trap on_exit EXIT

load_before=$(record "load average" load_average)
prepare_benchmeta

# Mirrors compareRows in benchmark/impls_test.go: each implementation's name, modes, and batches.
impl_table="ring SPSC,MPSC,SPMC,MPMC 1,32
ring-exact SPSC,MPSC,SPMC,MPMC 1,32
ring-single SPSC,MPSC,SPMC,MPMC 1
ring-rts MPSC,SPMC,MPMC 1,32
chan SPSC,MPSC,SPMC,MPMC 1
mutex SPSC,MPSC,SPMC,MPMC 1
xsync SPSC,MPSC,SPMC,MPMC 1
workiva SPSC,MPSC,SPMC,MPMC 1"

# Prints the leaf names BenchmarkCompare reports for impl at -test.cpu $own.
expected_rows() {
	printf '%s\n' "$impl_table" | awk -v impl="$1" -v cpu="$own" '
		$1 == impl {
			mode_count = split($2, modes, ",")
			batch_count = split($3, batches, ",")
			for (m = 1; m <= mode_count; m++) {
				for (b = 1; b <= batch_count; b++) {
					for (e = 1; e <= 2; e++) {
						elem = e == 1 ? "int64" : "ptr"
						printf "BenchmarkCompare/mode=%s/batch=%s/elem=%s/impl=%s-%s\n",
							modes[m], batches[b], elem, impl, cpu
					}
				}
			}
		}
	'
}

go_env=(env GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly)
compare_build=("${go_env[@]}" go -C benchmark test -c -o "$results/compare-unpadded.test" .)
padded_build=(
	"${go_env[@]}" go -C benchmark test -c -o "$results/compare-padded.test"
	-overlay "$results/overlay.json" .
)
race_build=("${go_env[@]}" go -C benchmark test -race -c -o "$results/compare-race.test" .)

build_compare() {
	local build_err

	if ! build_err=$(cd "$repo" && "$@" 2>&1 >/dev/null); then
		echo "$build_err" >&2
		die_on_build_error "the benchmark module" "the comparison binary" \
			"the comparison binary" "$build_err"
	fi
}

write_pad_overlay() {
	cat >"$results/layout_pad_test.go" <<EOF
package benchmark

// layoutPad shifts the binary's data layout, so runtime.sched moves off a slow offset. The init
// store keeps the linker from dropping it.
var layoutPad [$pad_count]*byte

func init() {
	layoutPad[0] = new(byte)
}
EOF
	printf '{"Replace": {"%s": "%s"}}\n' "$repo/benchmark/layout_pad_test.go" \
		"$results/layout_pad_test.go" >"$results/overlay.json"
}

pad_symbol=github.com/freshdresch/go-data-structures/benchmark.layoutPad

check_pad_symbol() {
	local nm_line=$1
	local size

	[ -n "$nm_line" ] || die "the padded binary has no $pad_symbol symbol"
	size=$(printf '%s\n' "$nm_line" | awk '{ print $2 }')
	[ "$size" = $((8 * pad_count)) ] ||
		die "$pad_symbol is $size bytes, want $((8 * pad_count)): $nm_line"
}

expected_all=""
while read -r implementation _; do
	expected_all="$expected_all$(expected_rows "$implementation")"$'\n'
done <<<"$impl_table"
printf '%s' "$expected_all" >"$results/provenance/expected-compare.txt"
snapshot_variant compare-unpadded "$repo" "$repo/benchmark" . \
	"$(git -C "$repo" rev-parse HEAD) working tree" false "" "${compare_build[@]}"
status="aborted building compare-unpadded"
build_compare "${compare_build[@]}"
if ! first_offset=$(sched_offset "$results/compare-unpadded.test" 2>/dev/null); then
	[ "$platform" != Darwin ] ||
		die "cannot read runtime.sched's offset, so the layout check cannot run"
	first_offset=unknown
fi
export_workload "$results/compare-unpadded.test" compare-unpadded "$own"
"$benchmeta" rows --manifest "$results/provenance/compare-unpadded.manifest.json" \
	--expected "$results/provenance/expected-compare.txt" --cpu "$own"
finalize_variant compare-unpadded "$results/compare-unpadded.test" "$first_offset"
selected_variant=compare-unpadded

pad_count=""
pad_nm_line=none
if [ -n "$pad" ]; then
	pad_count=$pad
elif [ "$platform" = Darwin ] && [ "$first_offset" = 96 ]; then
	pad_count=4
fi

final_offset=$first_offset
if [ -n "$pad_count" ]; then
	write_pad_overlay
	snapshot_variant compare-padded "$repo" "$repo/benchmark" . \
		"$(git -C "$repo" rev-parse HEAD) working tree" false "$results/overlay.json" \
		"${padded_build[@]}"
	status="aborted building compare-padded"
	build_compare "${padded_build[@]}"
	nm_output=$(go tool nm -size "$results/compare-padded.test") ||
		die "go tool nm failed on $results/compare.test"
	pad_nm_line=$(printf '%s\n' "$nm_output" | awk -v symbol="$pad_symbol" '$4 == symbol')
	check_pad_symbol "$pad_nm_line"
	final_offset=$(sched_offset "$results/compare-padded.test" 2>/dev/null) ||
		die "cannot read runtime.sched's offset in the padded binary"
	export_workload "$results/compare-padded.test" compare-padded "$own"
	"$benchmeta" rows --manifest "$results/provenance/compare-padded.manifest.json" \
		--expected "$results/provenance/expected-compare.txt" --cpu "$own"
	finalize_variant compare-padded "$results/compare-padded.test" "$final_offset"
	selected_variant=compare-padded
fi

if [ "$platform" = Darwin ] && [ "$final_offset" = 96 ]; then
	die "runtime.sched is at offset 96, the slow one, first offset $first_offset," \
		"padding ${pad_count:-none}; rerun with another -pad"
fi

snapshot_variant compare-race "$repo" "$repo/benchmark" . \
	"$(git -C "$repo" rev-parse HEAD) working tree" true "" "${race_build[@]}"
status="aborted building compare-race"
if ! race_err=$(cd "$repo" && "${race_build[@]}" 2>&1 >/dev/null); then
	echo "$race_err" >&2
	die "building the race binary failed, -race needs cgo and a C compiler, see the output above"
fi

export_workload "$results/compare-race.test" compare-race "$own"
"$benchmeta" rows --manifest "$results/provenance/compare-race.manifest.json" \
	--expected "$results/provenance/expected-compare.txt" --cpu "$own"
race_offset=$(sched_offset "$results/compare-race.test" 2>/dev/null) || race_offset=unknown
finalize_variant compare-race "$results/compare-race.test" "$race_offset"
cp "$results/$selected_variant.test" "$results/compare.test"
cmp "$results/$selected_variant.test" "$results/compare.test" || die "selected binary copy changed"
cp "$results/provenance/$selected_variant.json" "$results/provenance/compare.json"

gate1_names='TestCompareAccounting TestCompareCapacity TestWorkivaAbort TestPollingAbort'
gate1_names="$gate1_names TestRunFail TestRunLayout TestRingRTSRelaxed"
gate1_count=20
gate1_flags=(
	-test.timeout 5m
	-test.v
	-test.run "^(${gate1_names// /|})\$"
	-test.count "$gate1_count"
)
gate2_names='TestCompareAccounting TestWorkivaAbort TestPollingAbort'
gate2_count=5
gate2_flags=(
	-test.timeout 5m
	-test.v
	-test.run "^(${gate2_names// /|})\$"
	-test.count "$gate2_count"
)
gate3_names='TestCompareCapacity'
gate3_count=1
gate3_flags=(
	-test.timeout 5m
	-test.v
	-test.run "^(${gate3_names// /|})\$"
	-test.count "$gate3_count"
)

bench_flags() {
	local impl=$1

	printf '%s\n' -test.run '^$' -test.count 1 -test.benchtime "$benchtime" -test.benchmem \
		-test.cpu "$own" \
		-test.bench "^BenchmarkCompare\$/^mode=/^batch=/^elem=/^impl=$impl\$"
}

command_line() {
	local binary=$1
	shift
	if [ ${#pin[@]} -gt 0 ]; then
		printf '%q ' "${pin[@]}"
	fi
	printf '%s ' "$binary"
	printf '%q ' "$@"
	echo
}

build_line() {
	printf 'cd %q &&' "$repo"
	printf ' %q' "$@"
	echo
}

library_versions() {
	(cd "$repo" && "${go_env[@]}" go -C benchmark list -m \
		github.com/puzpuzpuz/xsync/v4 github.com/Workiva/go-datastructures)
}

write_state() {
	local tree_status
	local ring_status
	local module_status
	local ring_go_status
	local bench_args=()
	local arg

	while IFS= read -r arg; do
		bench_args+=("$arg")
	done < <(bench_flags '<impl>')

	echo "## script"
	git -C "$repo" rev-parse HEAD
	tree_status=$(git -C "$repo" status --porcelain --untracked-files=all -- benchmark/ scripts/)
	echo "benchmark/ and scripts/: ${tree_status:-clean}"
	ring_status=$(git -C "$repo" status --porcelain --untracked-files=no -- ring/)
	echo "ring/: ${ring_status:-clean}"
	module_status=$(git -C "$repo" status --porcelain -- go.mod go.sum)
	echo "root go.mod and go.sum: ${module_status:-clean}"
	ring_go_status=$(git -C "$repo" status --porcelain --untracked-files=all -- ':(glob)ring/*.go')
	echo "ring/*.go: ${ring_go_status:-clean}"

	echo "## binaries"
	record compare.test binary_info "$results/compare.test"
	record compare-race.test binary_info "$results/compare-race.test"

	echo "## libraries"
	record libraries library_versions

	echo "## layout"
	echo "first runtime.sched offset: $first_offset"
	echo "padding: ${pad_count:-none}"
	echo "layoutPad: $pad_nm_line"
	echo "final runtime.sched offset: $final_offset"

	echo "## commands"
	echo "cwd: $results"
	echo "build: $(build_line "${compare_build[@]}")"
	if [ -n "$pad_count" ]; then
		echo "padded build: $(build_line "${padded_build[@]}")"
	else
		echo "padded build: none"
	fi
	echo "race build: $(build_line "${race_build[@]}")"
	echo "gate 1: $(command_line "$results/compare.test" "${gate1_flags[@]}")"
	echo "gate 2: $(command_line "$results/compare-race.test" "${gate2_flags[@]}")"
	echo "gate 3: $(command_line "$results/compare.test" "${gate3_flags[@]}")"
	echo "rounds: $(command_line "$results/compare.test" "${bench_args[@]}")"
	echo "invocation limit: ${invocation_limit}s"
	echo "round count: $rounds"
	echo "benchtime: $benchtime"

	echo "## environment"
	record_environment

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

print_gate_failure() {
	local out=$1

	echo "--- FAIL and panic lines:"
	grep -E -- '--- FAIL|panic:' "$out" || true
	echo "last subtest started:"
	awk '/^=== RUN/ { last = $0 } END { if (last != "") print last }' "$out"
	if grep -q 'panic: test timed out' "$out"; then
		echo "tests running at the timeout:"
		awk '/^[ \t]*running tests:/ { p = 1 } p && /^$/ { exit } p' "$out"
	fi
	echo "last 20 lines:"
	tail -n 20 "$out"
}

run_gate() {
	local gate=$1
	local binary=$2
	local names=$3
	local count=$4
	local out="$results/gate$gate.txt"
	local exit_status=0
	local name
	local passes
	shift 4

	echo "gate $gate" >&2
	(cd "$results" && ${pin[@]+"${pin[@]}"} "$results/$binary" "$@") >"$out" 2>&1 ||
		exit_status=$?
	if [ "$exit_status" -ne 0 ] || grep -q -- '--- FAIL' "$out"; then
		print_gate_failure "$out" >&2
		die "gate $gate failed, exit status $exit_status, output in $out"
	fi
	# A renamed test makes -test.run match nothing, and the gate would pass without running it.
	for name in $names; do
		passes=$(grep -c -E -- "^--- PASS: $name \\(" "$out" || true)
		[ "$passes" -eq "$count" ] ||
			die "gate $gate passed $name $passes times, want $count, output in $out"
	done
}

status="aborted at the gates"
run_gate 1 compare.test "$gate1_names" "$gate1_count" "${gate1_flags[@]}"
run_gate 2 compare-race.test "$gate2_names" "$gate2_count" "${gate2_flags[@]}"
run_gate 3 compare.test "$gate3_names" "$gate3_count" "${gate3_flags[@]}"
{
	echo "## capacity"
	grep -F 'capacity: impl=' "$results/gate3.txt" || echo "none logged"
} >>"$state"

check_variant compare-unpadded
if [ "$selected_variant" = compare-padded ]; then check_variant compare-padded; fi
check_variant compare-race
cmp "$results/$selected_variant.test" "$results/compare.test" || die "selected binary changed"

findings=()
mkdir "$results/rounds"

add_finding() {
	findings+=("$1")
	warn "$1"
}

# Sleeps in the background and waits on it, because bash runs the TERM trap during wait but
# not during a foreground sleep, and the trap must kill the sleep so stopping leaves none behind.
watchdog() {
	local pid=$1
	local marker=$2
	local sleep_pid=""

	trap 'kill $sleep_pid 2>/dev/null || true; exit 0' TERM
	sleep "$invocation_limit" &
	sleep_pid=$!
	wait "$sleep_pid"
	: >"$marker"
	# SIGQUIT makes Go print every goroutine's stack before it exits.
	kill -QUIT "$pid" 2>/dev/null || exit 0
	sleep 10 &
	sleep_pid=$!
	wait "$sleep_pid"
	kill -KILL "$pid" 2>/dev/null || true
}

run_invocation() {
	local impl=$1
	local round=$2
	local out="$results/rounds/$round-$impl.txt"
	local marker="$out.timed-out"
	local exit_status=0
	local flags=()
	local arg
	local invocation_pid
	local watchdog_pid
	local difference

	while IFS= read -r arg; do
		flags+=("$arg")
	done < <(bench_flags "$impl")

	# Runs from the results directory, so anything the binary writes stays with the results. The
	# exec makes $! the binary's pid, or taskset's, which execs the binary under the same pid.
	(cd "$results" && exec ${pin[@]+"${pin[@]}"} "$results/compare.test" "${flags[@]}") \
		>"$out" 2>&1 &
	invocation_pid=$!
	watchdog "$invocation_pid" "$marker" &
	watchdog_pid=$!
	wait "$invocation_pid" || exit_status=$?
	kill "$watchdog_pid" 2>/dev/null || true
	wait "$watchdog_pid" 2>/dev/null || true

	if [ -e "$marker" ]; then
		add_finding "round $round $impl timed out after ${invocation_limit}s, sent SIGQUIT,\
 exit status $exit_status, output in $out"
		return
	fi
	if [ "$exit_status" -ne 0 ] || grep -q -- '--- FAIL' "$out"; then
		add_finding "round $round $impl failed, exit status $exit_status, output in $out"
		return
	fi
	if ! difference=$(diff_rows "$out" "$(expected_rows "$impl")"); then
		add_finding "round $round $impl has the wrong rows, output in $out$difference"
		return
	fi
	cat "$out" >>"$results/results.txt"
}

shuffled_impls() {
	printf '%s\n' "$impl_table" | awk '{ print $1 }' |
		awk -v seed="$1" 'BEGIN { srand(seed) } { printf "%.9f %s\n", rand(), $0 }' |
		sort -n | cut -d' ' -f2-
}

printf '## shuffle\nseed: %s\n' "$seed" >>"$state"
start=$(date +%s)
for ((k = 1; k <= rounds; k++)); do
	status="aborted at round $k"
	echo "round $k/$rounds" >&2
	order=$(shuffled_impls $((seed + k)))
	echo "round $k: $(printf '%s\n' "$order" | tr '\n' ' ')" >>"$state"
	for impl in $order; do
		run_invocation "$impl" "$k"
	done
	if [ "$k" -eq 1 ] && [ "$rounds" -gt 1 ]; then
		first_round=$(($(date +%s) - start))
		echo "round 1 took ${first_round}s, about $((first_round * (rounds - 1)))s remain" >&2
	fi
done
end=$(date +%s)
elapsed=$((end - start))

{
	echo "## findings"
	if [ ${#findings[@]} -eq 0 ]; then
		echo "none"
	else
		printf '%s\n' "${findings[@]}"
	fi
	echo "## timing"
	echo "rounds elapsed: ${elapsed}s"
	echo "total elapsed: ${SECONDS}s"
	record "load average after" load_average
	echo "## notes"
	echo "Layout: every row comes from one binary, so one placement of runtime.sched, recorded" \
		"above. A different placement could move the numbers, and possibly a close ranking."
	echo "Statistics: benchstat does not correct p-values across rows and metrics. A few rows" \
		"will show a significant difference by chance."
	echo "Rows: each row measures a queue together with how its callers wait. ring-single is" \
		"the like-for-like row for the libraries' single-element calls. The batch-32 ring rows" \
		"only show what batching gives the ring."
} >>"$state"

if [ ! -e "$results/results.txt" ]; then
	status="rounds complete, no results"
	die "no invocation succeeded, see the findings in $state"
fi

metrics='.unit:(sec/op OR B/s OR allocs/op OR B/op)'
status="rounds complete, comparison failed"
(cd "$results" && benchstat -filter "$metrics" \
	-col '/impl@(ring ring-exact ring-single ring-rts chan mutex xsync workiva)' results.txt) \
	>"$results/comparison.txt"
(cd "$results" && benchstat -filter "$metrics" \
	-col '/impl@(ring-single ring ring-exact ring-rts chan mutex xsync workiva)' results.txt) \
	>"$results/comparison-single.txt"
cat "$results/comparison.txt"
echo
cat "$results/comparison-single.txt"

echo
if [ ${#findings[@]} -gt 0 ]; then
	echo "findings:"
	printf '%s\n' "${findings[@]}"
fi
echo "results: $results"
echo "rounds elapsed: ${elapsed}s"
echo "total elapsed: ${SECONDS}s"
if [ ${#findings[@]} -gt 0 ]; then
	status="complete with findings"
else
	status=complete
fi
