#!/usr/bin/env bash
#
# Runs a quick, directional before-and-after comparison of the ring benchmarks for two committed
# refs. See scripts/README.md.

set -euo pipefail

prog=ring-quick-bench

usage() {
	echo "usage: scripts/ring-quick-bench.sh [-rounds N] [-benchtime D] <before-ref> <after-ref>"
	echo "       scripts/ring-quick-bench.sh [-rounds N] [-benchtime D] -reuse <dir>"
	echo "       optional: -methodology-comparison -allow-layout-confounded"
}

usage_error() {
	echo "$prog: $*" >&2
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

rounds=10
benchtime=200ms
reuse_dir=""
methodology_comparison=false
allow_layout_confounded=false
refs=()

while [ $# -gt 0 ]; do
	case "$1" in
	-h)
		usage
		exit 0
		;;
	-methodology-comparison | -allow-layout-confounded)
		case "$1" in
		-methodology-comparison)
			[ "$methodology_comparison" = false ] || usage_error "duplicate $1"
			methodology_comparison=true
			;;
		-allow-layout-confounded)
			[ "$allow_layout_confounded" = false ] || usage_error "duplicate $1"
			allow_layout_confounded=true
			;;
		esac
		shift
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
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
check_tools
check_ac_power

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)

select_cpus

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
build_dirs=()

on_exit() {
	echo "status: $status" >>"$state"
	for build_dir in ${build_dirs[@]+"${build_dirs[@]}"}; do
		rm -rf "$build_dir"
	done
}
trap on_exit EXIT

load_before=$(record "load average" load_average)

build_side() {
	local side=$1
	local hash=$2
	local build_err

	build_dir=$(mktemp -d)
	build_dirs+=("$build_dir")
	git -C "$repo" archive "$hash" | tar -x -C "$build_dir"
	snapshot_variant "$side" "$build_dir" "$build_dir" ./ring "$hash" false "" \
		env GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly \
		go test -c -o "$results/$side.test" ./ring
	status="aborted building $side"
	if ! build_err=$(
		cd "$build_dir" &&
			GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly \
				go test -c -o "$results/$side.test" ./ring 2>&1 >/dev/null
	); then
		echo "$build_err" >&2
		die_on_build_error "the $side ref" "$side" "$side ($hash)" "$build_err"
	fi
	export_workload "$results/$side.test" "$side-single" 1
	export_workload "$results/$side.test" "$side" "$own"
	local offset
	offset=$(sched_offset "$results/$side.test" 2>/dev/null) || offset=unknown
	finalize_variant "$side" "$results/$side.test" "$offset"
	build_dir=""
}

if [ -n "$reuse_dir" ]; then
	[ -d "$reuse_dir/provenance" ] || die "legacy results need fresh builds for this protocol"
	cp -R "$reuse_dir/provenance" "$results/"
fi
prepare_benchmeta

admission_flags=()
if [ "$methodology_comparison" = true ]; then
	admission_flags+=(--methodology-comparison)
fi
if [ "$allow_layout_confounded" = true ]; then
	admission_flags+=(--allow-layout-confounded)
fi

admit_comparison() {
	status="aborted at admission"
	"$benchmeta" admit --before "$results/provenance/before.json" \
		--after "$results/provenance/after.json" \
		--current-runner "$results/provenance/current-runner.json" \
		--platform "$results/provenance/platform.json" --output "$results/provenance/admission.json" \
		${admission_flags[@]+"${admission_flags[@]}"} | tee -a "$state"
}

if [ -z "$reuse_dir" ]; then
	build_side before "$before_hash"
	build_side after "$after_hash"
else
	cp "$reuse_dir/before.test" "$reuse_dir/after.test" "$results/"
	echo "reuse-parent: $reuse_dir" >>"$state"
	admit_comparison
	for side in before after; do
		export_workload "$results/$side.test" "$side-reuse-single" 1
		export_workload "$results/$side.test" "$side-reuse" "$own"
		cmp "$results/provenance/$side.manifest.json" \
			"$results/provenance/$side-reuse.manifest.json" || die "reused binary workload differs"
		cmp "$results/provenance/$side-single.manifest.json" \
			"$results/provenance/$side-reuse-single.manifest.json" ||
			die "reused sequential workload differs"
	done
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

if [ "$own" -eq 1 ]; then
	own_rows=$(printf '%s\n' "$own_rows" | sed 's/-1$//')
fi
printf '%s\n' "$single_rows" >"$results/provenance/expected-single.txt"
printf '%s\n' "$own_rows" >"$results/provenance/expected-own.txt"
for side in before after; do
	"$benchmeta" rows --manifest "$results/provenance/$side-single.manifest.json" \
		--expected "$results/provenance/expected-single.txt" --cpu 1
	"$benchmeta" rows --manifest "$results/provenance/$side.manifest.json" \
		--expected "$results/provenance/expected-own.txt" --cpu "$own"
done
admit_comparison
if [ -z "$reuse_dir" ]; then
	check_variant before
	check_variant after
	for build_dir in ${build_dirs[@]+"${build_dirs[@]}"}; do
		rm -rf "$build_dir"
	done
	build_dirs=()
fi
interpretation=implementation-comparison
if [ "$methodology_comparison" = true ]; then
	interpretation=methodology-comparison
fi
if grep -q '"layout-confounded"' "$results/provenance/admission.json"; then
	interpretation="$interpretation layout-confounded"
fi

command_line() {
	if [ ${#pin[@]} -gt 0 ]; then
		printf '%q ' "${pin[@]}"
	fi
	printf '%s ' "$results/<side>.test"
	printf '%q ' "$@"
	echo
}

write_state() {
	local scripts_status
	local side

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
		record "$side runtime.sched offset" sched_offset "$results/$side.test"
	done

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
before_sched=$(sched_offset "$results/before.test" 2>/dev/null) || before_sched=unknown
after_sched=$(sched_offset "$results/after.test" 2>/dev/null) || after_sched=unknown
mkdir "$results/rounds"

check_rows() {
	local out=$1
	local expected=$2
	local difference

	if ! difference=$(diff_rows "$out" "$expected"); then
		die "unexpected rows in $out" "$difference"
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
		echo "round $k order: before after" >>"$state"
		run_side before "$k"
		run_side after "$k"
	else
		echo "round $k order: after before" >>"$state"
		run_side after "$k"
		run_side before "$k"
	fi
done
end=$(date +%s)
elapsed=$((end - start))
printf '## timing\nelapsed: %ss\n' "$elapsed" >>"$state"

status="rounds complete, comparison failed"
for side in before after; do
	"$benchmeta" project --input "$results/$side.txt" --output "$results/$side-primary.txt"
done
presentation_header() {
	echo "interpretation: $interpretation"
	echo "Concurrent RingAPI primary: ns/elem per verified transfer. Sequential ns/op: per pair."
	echo "Throughput ns/op: per transfer. Handoff ns/op: per round trip."
	echo "Raw ns/op, MB/s, B/op and allocs/op retain the Go iteration denominator."
	echo "Bulk elem/op records actual transfers per Go iteration."
}
metrics='.unit:(sec/elem OR sec/op OR handoff-p50 OR handoff-p90 OR handoff-p99)'
{
	presentation_header
	(cd "$results" && benchstat -filter "$metrics" before-primary.txt after-primary.txt)
} >"$results/comparison.txt"
{
	presentation_header
	(cd "$results" && benchstat -filter '.unit:(sec/op OR B/s OR allocs/op OR B/op OR elem/op)' \
		before.txt after.txt)
} >"$results/comparison-raw.txt"
cat "$results/comparison.txt"

echo
if [ "$harness_diff" != none ]; then
	echo "The benchmark files differ between the refs, so harness changes are part of the" \
		"comparison. The diff stat follows."
	echo "$harness_diff"
fi
if [ "$before_sched" != "$after_sched" ] || [ "$before_sched" = unknown ]; then
	echo "The binaries place runtime.sched at different offsets within a 128-byte line," \
		"before $before_sched and after $after_sched. On Apple silicon that alone has moved" \
		"the MPMC rows by 30% or more, with offset 96 the slow one seen, so an MPMC change" \
		"here may not come from the code."
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
