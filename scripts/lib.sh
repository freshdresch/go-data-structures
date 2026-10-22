# shellcheck shell=bash
# shellcheck disable=SC2154
#
# Helpers shared by the benchmark scripts. This file defines functions only, and nothing runs when
# it is sourced. The sourcing script sets prog and platform first. The file-wide SC2154 disable
# lets shellcheck accept those two names, but also hides any other unassigned name in this file,
# typos included.

die() {
	echo "$prog: $*" >&2
	exit 1
}

warn() {
	echo "$prog: warning: $*" >&2
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
		lscpu | grep '^Model name'
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
