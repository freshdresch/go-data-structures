# shellcheck shell=bash
# shellcheck disable=SC2154
#
# Helpers shared by the benchmark scripts. This file defines functions and the go_env array, and
# nothing runs when it is sourced. The sourcing script sets prog and platform first. The file-wide
# SC2154 disable lets shellcheck accept those two names, but also hides any other unassigned name
# in this file, typos included. add_finding appends to the caller's global findings array, and
# check_positive_integer calls the sourcing script's usage_error.

die() {
	echo "$prog: $*" >&2
	exit 1
}

warn() {
	echo "$prog: warning: $*" >&2
}

# The sourcing script reads go_env.
# shellcheck disable=SC2034
go_env=(env GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly)

add_finding() {
	findings+=("$1")
	warn "$1"
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

# Prints the names read on stdin in an order fixed by seed. The order repeats only with the same
# awk implementation, because each awk's srand and rand differ.
shuffle() {
	awk -v seed="$1" 'BEGIN { srand(seed) } { printf "%.9f %s\n", rand(), $0 }' |
		LC_ALL=C sort -n | cut -d' ' -f2-
}

# Sends first_signal to pid after limit seconds, then KILL 10 seconds later, and creates marker
# when the limit passes. first_signal is QUIT for Go programs, which print their stacks, or TERM
# for Rust programs, because under WSL's crash-capture core_pattern a QUIT would send a Rust core
# dump to the collector. The caller runs watchdog in the background, because it installs its own
# TERM trap, and stops it with TERM. It sleeps in the background and waits on it, because bash
# runs the TERM trap during wait but not during a foreground sleep, and the trap must kill the
# sleep so stopping leaves none behind.
watchdog() {
	local pid=$1
	local marker=$2
	local limit=$3
	local first_signal=$4
	local sleep_pid=""

	case "$first_signal" in
	QUIT | TERM) ;;
	*)
		# The watchdog runs in the background, so exiting alone would leave the invocation
		# without a time limit. Killing the invocation makes the mistake fail it visibly.
		echo "$prog: watchdog: unsupported signal $first_signal" >&2
		kill -KILL "$pid" 2>/dev/null || true
		exit 1
		;;
	esac
	# Kills every background job, so a TERM that arrives before sleep_pid is set still kills
	# the sleep.
	trap 'kill $(jobs -p) 2>/dev/null || true; exit 0' TERM
	sleep "$limit" &
	sleep_pid=$!
	wait "$sleep_pid"
	: >"$marker"
	kill "-$first_signal" "$pid" 2>/dev/null || exit 0
	sleep 10 &
	sleep_pid=$!
	wait "$sleep_pid"
	kill -KILL "$pid" 2>/dev/null || true
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

check_tools() {
	local tools
	local missing
	local tool

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
}

# Keeps the first CPU listed for each (socket, core) pair, so each core gets one thread.
one_cpu_per_core() {
	lscpu -p=CPU,CORE,SOCKET |
		awk -F, '!/^#/ && !seen[$3 "," $2]++ { printf "%s%s", sep, $1; sep = "," }'
}

# The sourcing script reads the pin, own_cpus, and own globals this sets.
# shellcheck disable=SC2034
select_cpus() {
	local ncpu

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
}

binary_info() {
	go version -m "$1" | awk -F'\t' '
		NR == 1 { sub(/^[^:]*: /, ""); print "toolchain=" $0 }
		$2 == "build" && $3 ~ /^(GOOS|GOARCH|GOARM64|GOAMD64|CGO_ENABLED)=/ { print $3 }
	'
}

# Prints the offset of runtime.sched within a 128-byte line. The scheduler's lock and run
# queue live there, the benchmarks yield constantly, and on Apple silicon the MPMC rows have
# moved by 30% or more with this offset alone.
sched_offset() {
	local addr
	addr=$(go tool nm "$1" | awk '$3 == "runtime.sched" { print $1 }')
	[ -n "$addr" ] || return 1
	echo $((16#$addr % 128))
}

cpu_model() {
	if [ "$platform" = Darwin ]; then
		sysctl -n machdep.cpu.brand_string
	else
		lscpu | awk '/^Model name/ { sub(/^[^:]*:[ \t]*/, ""); sub(/[ \t]+$/, ""); print }'
	fi
}

benchstat_module() {
	go version -m "$(command -v benchstat)" | awk '$1 == "mod"'
}

record_environment() {
	local any_set=no
	local name

	for name in GOGC GODEBUG GOMEMLIMIT GOMAXPROCS; do
		if [ -n "${!name+x}" ]; then
			warn "$name is set to '${!name}'"
			echo "$name=${!name}"
			any_set=yes
		fi
	done
	[ "$any_set" = yes ] || echo "none set"
}

die_on_build_error() {
	local newer_go_subject=$1
	local build_label=$2
	local failure_label=$3
	local build_err=$4

	case "$build_err" in
	*GOTOOLCHAIN=local*)
		die "$newer_go_subject needs a newer Go than the local toolchain"
		;;
	*"cannot find module"* | *"dial tcp"* | *"missing go.sum entry"*)
		die "building $build_label failed, the build needs the module cache to hold" \
			"the dependencies, or network access"
		;;
	*)
		die "building $failure_label failed, see the output above"
		;;
	esac
}

diff_rows() {
	local got_file=$1
	local expected=$2
	local got
	local missing_rows
	local extra_rows

	# Result rows have the iteration count as their second field, unlike headers or log lines.
	got=$(awk '$1 ~ /^Benchmark/ && $2 ~ /^[0-9]+$/ { print $1 }' "$got_file" | sort)
	expected=$(printf '%s\n' "$expected" | sort)
	missing_rows=$(comm -23 <(echo "$expected") <(echo "$got"))
	extra_rows=$(comm -13 <(echo "$expected") <(echo "$got"))
	if [ -n "$missing_rows" ] || [ -n "$extra_rows" ]; then
		printf '%s' $'\nmissing:\n'"${missing_rows:-none}"$'\nextra:\n'"${extra_rows:-none}"
		return 1
	fi
}

check_ac_power() {
	power_preflight="not applicable"
	if [ "$platform" = Darwin ]; then
		local power
		power=$(pmset -g batt) || die "could not read AC-power state"
		case "$power" in
		*"AC Power"*) power_preflight="AC Power" ;;
		*) die "not on AC power, pmset -g batt reports: $power" ;;
		esac
	fi
}

json_string() {
	local value=$1
	value=${value//\\/\\\\}
	value=${value//\"/\\\"}
	value=${value//$'\n'/\\n}
	value=${value//$'\r'/\\r}
	value=${value//$'\t'/\\t}
	printf '"%s"' "$value"
}

json_array() {
	local separator=""
	local value
	printf '['
	for value in "$@"; do
		printf '%s' "$separator"
		json_string "$value"
		separator=,
	done
	printf ']'
}

prepare_benchmeta() {
	mkdir -p "$results/provenance"
	benchmeta="$results/benchmeta"
	GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly \
		go -C "$repo" build -o "$benchmeta" ./scripts/benchmeta ||
		die "building metadata helper failed"
	runner_bundle=("$repo/scripts/$prog.sh" "$repo/scripts/lib.sh")
	local source
	for source in "$repo"/scripts/benchmeta/*.go; do
		case "$source" in *_test.go) continue ;; esac
		runner_bundle+=("$source")
	done
	{
		printf '{"runner_bundle":'
		json_array "${runner_bundle[@]}"
		printf '}\n'
	} >"$results/provenance/current-runner.json"
	printf '{"os":%s,"architecture":%s}\n' \
		"$(json_string "$platform")" "$(json_string "$(go env GOARCH)")" \
		>"$results/provenance/platform.json"
	go version -m "$benchmeta" >"$results/provenance/benchmeta-build-info.txt"
	{
		echo "## metadata helper"
		echo "binary: $benchmeta"
		echo "power preflight: $power_preflight"
		echo "selected GOMAXPROCS: $own"
		echo "selected CPUs: ${own_cpus:-OS scheduler}"
		echo "WSL virtual CPUs and Apple mixed cores can limit affinity interpretation"
	} >>"$state"
}

snapshot_variant() {
	local variant=$1
	local source_root=$2
	local package_dir=$3
	local target=$4
	local origin=$5
	local race=$6
	local overlay=$7
	shift 7
	local request="$results/provenance/$variant.request.json"
	status="aborted snapshotting $variant"
	{
		printf '{"schema":1,"runner_kind":%s,"origin":%s,' \
			"$(json_string "$prog")" "$(json_string "$origin")"
		printf '"source_root":%s,"package_dir":%s,"package_target":%s,' \
			"$(json_string "$source_root")" "$(json_string "$package_dir")" "$(json_string "$target")"
		printf '"helper_binary":%s,"variant":%s,"runner_bundle":' \
			"$(json_string "$benchmeta")" "$(json_string "$variant")"
		json_array "${runner_bundle[@]}"
		printf ',"settings":{"race":%s,"overlay":%s,"environment":{' \
			"$race" "$(json_string "$overlay")"
		printf '"GOTOOLCHAIN":"local","GOWORK":"off","GOFLAGS":"-mod=readonly"}},'
		printf '"build_command":'
		json_array "$@"
		printf '}\n'
	} >"$request"
	echo "snapshot: $variant" >>"$state"
	"$benchmeta" snapshot --request "$request" \
		--output "$results/provenance/$variant.snapshot.json"
}

export_workload() {
	local binary=$1
	local variant=$2
	local cpu=$3
	local stem="$results/provenance/$variant"
	status="aborted exporting $variant metadata"
	"$binary" -test.run '^TestBenchmarkManifest$' -test.v -test.cpu "$cpu" \
		-test.count 1 -test.timeout 5m >"$stem.log" 2>&1 ||
		die "metadata export failed: $stem.log"
	local count
	count=$(grep -c 'BENCHMARK_MANIFEST ' "$stem.log" || true)
	[ "$count" -eq 1 ] || die "metadata export needs exactly one payload: $stem.log"
	grep -q '^--- PASS: TestBenchmarkManifest (' "$stem.log" ||
		die "metadata test did not explicitly pass: $stem.log"
	sed -n 's/^.*BENCHMARK_MANIFEST //p' "$stem.log" >"$stem.manifest.json"
}

finalize_variant() {
	local variant=$1
	local binary=$2
	local offset=$3
	local stem="$results/provenance/$variant"
	status="aborted finalizing $variant"
	go version -m "$binary" >"$stem.build-info.txt" || die "cannot read binary build information"
	{
		printf '{"binary":%s,"build_info":%s,"manifest":%s,"manifest_log":%s,' \
			"$(json_string "../$(basename "$binary")")" \
			"$(json_string "$variant.build-info.txt")" \
			"$(json_string "$variant.manifest.json")" "$(json_string "$variant.log")"
		printf '"scheduler_offset":%s' "$(json_string "$offset")"
		if [ "$prog" = ring-quick-bench ]; then
			printf ',"single_manifest":%s,"single_manifest_log":%s' \
				"$(json_string "$variant-single.manifest.json")" "$(json_string "$variant-single.log")"
		fi
		printf '}\n'
	} >"$stem.artifacts.json"
	echo "finalize: $variant offset=$offset" >>"$state"
	"$benchmeta" finalize --snapshot "$stem.snapshot.json" --artifacts "$stem.artifacts.json" \
		--output "$stem.json" --report "$stem.verification.json"
}

check_variant() {
	local variant=$1
	local stem="$results/provenance/$variant"
	# The sourcing runner reads status in its EXIT trap.
	# shellcheck disable=SC2034
	status="aborted checking $variant before timing"
	"$benchmeta" finalize --check-only --snapshot "$stem.snapshot.json" \
		--artifacts "$stem.artifacts.json" --output "$stem.json" \
		--report "$stem.pretiming-verification.json"
}
