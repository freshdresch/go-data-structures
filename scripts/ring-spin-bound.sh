#!/usr/bin/env bash
#
# Compares the MPSC, SPMC, and MPMC throughput of one committed ref built with different publish
# wait spin bounds, the defaultSpinsBeforeYield constant in ring/dpdk/ring.go (ring/ring.go in
# older commits), or tailSpins in commits before it was renamed. The first bound is the base, and the other sides follow in the order
# given, in the builds, the rotation, and the comparison. Run it to choose the spin bound after a
# change to the publish wait or to the ring's layout. See scripts/README.md.

set -euo pipefail

usage() {
	echo "usage: scripts/ring-spin-bound.sh [-rounds N] [-benchtime D] <ref> <bound> <bound>..."
}

die() {
	echo "ring-spin-bound: $*" >&2
	exit 1
}

usage_error() {
	echo "ring-spin-bound: $*" >&2
	usage >&2
	exit 1
}

# Prints the value in base 10, so a leading zero such as 08 is not read as octal, or fails when
# the value is not a positive integer.
positive_integer() {
	case "$1" in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ $((10#$1)) -gt 0 ] || return 1
	echo $((10#$1))
}

rounds=20
benchtime=200ms
args=()

while [ $# -gt 0 ]; do
	case "$1" in
	-h)
		usage
		exit 0
		;;
	-rounds | -benchtime)
		[ $# -ge 2 ] || usage_error "$1 needs a value"
		case "$1" in
		-rounds) rounds=$2 ;;
		-benchtime) benchtime=$2 ;;
		esac
		shift 2
		;;
	-*)
		usage_error "unknown flag $1"
		;;
	*)
		args+=("$1")
		shift
		;;
	esac
done

rounds=$(positive_integer "$rounds") ||
	usage_error "-rounds must be a positive integer, got '$rounds'"
[ ${#args[@]} -ge 3 ] || usage_error "need a ref and at least two bounds"

ref=${args[0]}
bounds=()
for arg in "${args[@]:1}"; do
	bound=$(positive_integer "$arg") || usage_error "bounds must be positive integers, got '$arg'"
	for seen in ${bounds[@]+"${bounds[@]}"}; do
		[ "$seen" != "$bound" ] || usage_error "bound $bound is given more than once"
	done
	bounds+=("$bound")
done

platform=$(uname -s)
case "$platform" in
Darwin) tools="git go benchstat" ;;
Linux) tools="git go benchstat lscpu taskset" ;;
*) die "unsupported platform $platform" ;;
esac
for tool in $tools; do
	command -v "$tool" >/dev/null 2>&1 || die "missing tool $tool"
done

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
hash=$(git -C "$repo" rev-parse --verify --quiet "$ref^{commit}") || die "cannot resolve ref '$ref'"

pin=()
if [ "$platform" = Darwin ]; then
	own=10
	[ "$(sysctl -n hw.ncpu)" -ge "$own" ] || die "need at least $own CPUs"
	cpus="all"
else
	# One CPU per (socket, core) pair, so each core runs one thread.
	cpus=$(lscpu -p=CPU,CORE,SOCKET |
		awk -F, '!/^#/ && !seen[$3 "," $2]++ { printf "%s%s", sep, $1; sep = "," }')
	own=$(printf '%s\n' "$cpus" | tr ',' '\n' | wc -l | tr -d ' ')
	[ "$own" -ge 8 ] || die "need at least 8 physical cores for the MPMC rows, found $own"
	pin=(taskset -c "$cpus")
fi

# MPSC and SPMC have 5 goroutines and MPMC has 8. Each row either fits its goroutines or runs on
# one CPU. The lists drop duplicates, so on an 8-core machine the own-CPU setting is not repeated.
single_writer_cpus=$(printf '%s\n' 1 5 8 "$own" | awk '!seen[$0]++' | paste -sd, -)
multi_writer_cpus=$(printf '%s\n' 1 8 "$own" | awk '!seen[$0]++' | paste -sd, -)

mkdir -p "$HOME/ring-spin"
out="$HOME/ring-spin/$(date +%Y%m%d-%H%M%S)-$$"
mkdir "$out"

build() {
	local name=$1
	local spins=$2
	local dir="$out/src-$name"
	local spins_line='^const (tailSpins|defaultSpinsBeforeYield) = [0-9]+$'

	mkdir "$dir"
	git -C "$repo" archive "$hash" | tar -x -C "$dir"
	# The DPDK-style queues moved from ring to ring/dpdk. Older refs keep them in ring.
	local package=ring
	if [ -f "$dir/ring/dpdk/ring.go" ]; then
		package=ring/dpdk
	fi
	local source_file="$dir/$package/ring.go"
	grep -Eq "$spins_line" "$source_file" ||
		die "$package/ring.go at $hash has no line matching '$spins_line'"
	sed -E -i.orig "s/$spins_line/const \1 = $spins/" "$source_file"
	grep -Eq "^const (tailSpins|defaultSpinsBeforeYield) = $spins$" "$source_file" ||
		die "$name edit did not apply"
	(cd "$dir" && GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly \
		go test -c -o "$out/$name.test" "./$package")
}

sides=()
for bound in "${bounds[@]}"; do
	sides+=("s$bound")
	build "s$bound" "$bound"
done

{
	echo "## machine"
	uname -a
	if [ "$platform" = Darwin ]; then
		sysctl -n machdep.cpu.brand_string
		pmset -g batt | head -1
		echo "load average: $(sysctl -n vm.loadavg)"
	else
		lscpu | grep '^Model name' || true
		echo "nproc: $(nproc)"
		echo "own CPUs ($own): $cpus"
		echo "load average: $(cat /proc/loadavg)"
	fi
	go version
	echo "## run"
	echo "ref: $ref"
	echo "commit: $hash"
	echo "bounds: ${bounds[0]} (base) ${bounds[*]:1}"
	echo "rounds: $rounds"
	echo "benchtime: $benchtime"
	echo "MPSC and SPMC at -cpu $single_writer_cpus, MPMC at -cpu $multi_writer_cpus"
} >"$out/state.txt"
cat "$out/state.txt"

single_writer='^BenchmarkThroughput$/^mode=(MPSC|SPMC)$/^batch=(1|32)$/^elem=int64$/^impl=ring$'
multi_writer='^BenchmarkThroughput$/^mode=MPMC$/^batch=(1|32)$/^elem=int64$/^impl=ring$'

run() {
	local side=$1
	local pattern=$2
	local cpu_list=$3
	${pin[@]+"${pin[@]}"} "./$side.test" -test.run '^$' -test.bench "$pattern" \
		-test.benchtime "$benchtime" -test.count 1 -test.cpu "$cpu_list" -test.benchmem \
		>>"$side.txt"
}

cd "$out"
start=$(date +%s)
for ((k = 0; k < rounds; k++)); do
	echo "round $((k + 1))/$rounds" >&2
	for ((i = 0; i < ${#sides[@]}; i++)); do
		side=${sides[$(((k + i) % ${#sides[@]}))]}
		run "$side" "$single_writer" "$single_writer_cpus"
		run "$side" "$multi_writer" "$multi_writer_cpus"
	done
done
echo "elapsed: $(($(date +%s) - start))s" | tee -a state.txt
if [ "$platform" = Darwin ]; then
	pmset -g batt | head -1 | tee -a state.txt
fi
side_files=()
for side in "${sides[@]}"; do
	printf '%s rows: ' "$side"
	grep -c '^Benchmark' "$side.txt"
	side_files+=("$side.txt")
done
benchstat -filter '.unit:sec/op' "${side_files[@]}" | tee comparison.txt
echo "results: $out"
