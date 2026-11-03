#!/usr/bin/env bash
#
# Times the ring queues against Go channels and crossbeam in crossbeam's benchmark suite, in
# shuffled rounds, and summarizes the samples with benchstat. See benchmark/crossbeam/README.md.

set -euo pipefail

command_line=$(printf '%q ' "$0" "$@")
prog=crossbeam-bench
platform=$(uname -s)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

usage() {
	echo "usage: scripts/crossbeam-bench.sh [-rounds N] [-seed N] [-timeout S]"
	echo
	echo "  -rounds N   rounds to run, default 20"
	echo "  -seed N     shuffle seed, default the script's pid; round r uses seed + r, and a seed"
	echo "              repeats an order only with the same awk implementation"
	echo "  -timeout S  seconds each invocation may run, default 120"
}

usage_error() {
	echo "$prog: $*" >&2
	usage >&2
	exit 1
}

rounds=20
seed=$$
invocation_limit=120

while [ $# -gt 0 ]; do
	case "$1" in
	-h)
		usage
		exit 0
		;;
	-rounds | -seed | -timeout)
		[ $# -ge 2 ] || usage_error "$1 needs a value"
		case "$1" in
		-rounds) rounds=$2 ;;
		-seed) seed=$2 ;;
		-timeout) invocation_limit=$2 ;;
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
check_positive_integer -timeout "$invocation_limit"
invocation_limit=$((10#$invocation_limit))

# One line per kind: the kind as <Lang>-<impl>, and the topologies it runs.
invocation_table="Rust-crossbeam-channel seq,spsc,mpsc,mpmc
Rust-arrayqueue seq,spsc,mpsc,mpmc
Rust-arrayqueue-backoff seq,spsc,mpsc,mpmc
Go-chan seq,spsc,mpsc,mpmc
Go-dpdk seq,spsc,mpsc,mpmc
Go-frontier mpsc,mpmc
Go-dpdk-mpmc seq,spsc,mpsc
Go-frontier-mpmc seq,spsc,mpsc"

prefixes="bounded1 bounded1024 bounded2p18 bounded2p23"
invocation_count=91
# Each cell needs this many kept samples for benchstat's 95% confidence interval.
min_samples=6

# The benchstat -row projection every summary uses.
summary_rows="/cap@($prefixes) /topo@(seq spsc mpsc mpmc)"

# Prints one "<kind> <workload>" line per invocation of a round. seq runs only at bounded2p23.
expand_invocations() {
	printf '%s\n' "$invocation_table" | awk -v prefixes="$prefixes" '
		BEGIN { prefix_count = split(prefixes, prefix, " ") }
		{
			topology_count = split($2, topology, ",")
			for (t = 1; t <= topology_count; t++) {
				if (topology[t] == "seq") {
					print $1, "bounded2p23_seq"
					continue
				}
				for (p = 1; p <= prefix_count; p++) {
					print $1, prefix[p] "_" topology[t]
				}
			}
		}
	'
}

kinds() {
	printf '%s\n' "$invocation_table" | awk '{ print $1 }'
}

go_kinds() {
	kinds | grep '^Go-'
}

# Prints the benchstat -col projection with first as the first column and the other kinds after
# it in table order.
summary_columns() {
	local first=$1
	local others

	others=$(kinds | awk -v first="$first" '$1 != first' | tr '\n' ' ')
	printf '/impl@(%s %s)\n' "$first" "${others% }"
}

# Prints the command for one invocation, one argument per line.
invocation_command() {
	local kind=$1
	local workload=$2

	case "$kind" in
	Rust-*) printf '%s\n' "$results/target/release/${kind#Rust-}" "$workload" ;;
	Go-chan) printf '%s\n' "$results/bin/gochan" "$workload" ;;
	Go-*) printf '%s\n' "$results/bin/ring" "${kind#Go-}" "$workload" ;;
	*) return 1 ;;
	esac
}

actual_count=$(expand_invocations | wc -l | tr -d ' ')
[ "$actual_count" -eq "$invocation_count" ] ||
	die "the invocation table expands to $actual_count invocations, want $invocation_count"

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
crate="$repo/benchmark/crossbeam"

# Preflight.
case "$platform" in
Darwin) tools="git go benchstat cargo rustc" ;;
Linux) tools="git go benchstat cargo rustc lscpu taskset" ;;
*) die "unsupported platform $platform" ;;
esac
missing=""
for tool in $tools; do
	command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
done
[ -z "$missing" ] || die "missing tools:$missing"
command -v uv >/dev/null 2>&1 || warn "uv is missing, so the plot will be skipped"
check_ac_power

rustc_version=$(cd "$crate" && rustc -V) || die "rustc -V failed in $crate"
case "$rustc_version" in
"rustc 1.99.1 "*) ;;
*) die "rustc -V in $crate prints '$rustc_version', want 1.99.1" ;;
esac

# Each changes one language's rows at run time or how the Rust programs are compiled, and leaves
# no trace that the build checks can catch.
for name in GOMAXPROCS GOGC GODEBUG GOMEMLIMIT GOTRACEBACK GLIBC_TUNABLES LD_PRELOAD RUSTFLAGS \
	CARGO_BUILD_RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER; do
	[ -z "${!name+x}" ] || die "$name is set, unset it before timing"
done
refused_names=$(compgen -e | grep -E '^(CARGO_PROFILE_|CARGO_BUILD_RUSTC|MALLOC_)' || true)
[ -z "$refused_names" ] ||
	die "unset these before timing: $(printf '%s\n' "$refused_names" | tr '\n' ' ')"

# mpmc runs 8 workers, so every invocation gets 8 CPUs.
needed_cpus=8
pin=()
pin_cpus=""
if [ "$platform" = Darwin ]; then
	ncpu=$(sysctl -n hw.ncpu)
	[ "$ncpu" -ge "$needed_cpus" ] || die "need at least $needed_cpus CPUs, hw.ncpu is $ncpu"
else
	core_cpus=$(one_cpu_per_core) || die "could not read the core map from lscpu"
	core_count=$(printf '%s\n' "$core_cpus" | tr ',' '\n' | grep -c . || true)
	[ "$core_count" -ge "$needed_cpus" ] ||
		die "the one-thread-per-core set has $core_count CPUs, need at least $needed_cpus"
	pin_cpus=$(printf '%s\n' "$core_cpus" | tr ',' '\n' | head -n "$needed_cpus" | paste -sd, -)
	pin=(taskset -c "$pin_cpus")
fi

mkdir -p "$HOME/crossbeam-compare"
results="$HOME/crossbeam-compare/$(date +%Y%m%d-%H%M%S)-$$"
mkdir "$results"
state="$results/state.txt"

status="aborted at the build"
invocation_pid=""
watchdog_pid=""
scratch=""

# Stopping the wrapper leaves no invocation or watchdog behind. The watchdog gets TERM, because
# its TERM trap kills its sleep, and a KILL would orphan that sleep.
on_exit() {
	if [ -n "$invocation_pid" ]; then
		kill -KILL "$invocation_pid" 2>/dev/null || true
		wait "$invocation_pid" 2>/dev/null || true
	fi
	if [ -n "$watchdog_pid" ]; then
		kill -TERM "$watchdog_pid" 2>/dev/null || true
		wait "$watchdog_pid" 2>/dev/null || true
	fi
	if [ -n "$scratch" ]; then
		rm -rf "$scratch"
	fi
	echo "status: $status" >>"$state"
}
on_signal() {
	status=interrupted
	exit 1
}
trap on_exit EXIT
trap on_signal INT TERM HUP

# Build and test.
build_start=$SECONDS
echo "building" >&2
mkdir "$results/bin"
cargo_build_log="$results/cargo-build.txt"
if ! (cd "$crate" && cargo build --release --frozen -v --target-dir "$results/target") \
	>"$cargo_build_log" 2>&1; then
	cat "$cargo_build_log" >&2
	die "cargo build failed, see the output above"
fi
for program in gochan ring; do
	if ! build_err=$(cd "$repo/benchmark" &&
		"${go_env[@]}" go build -o "$results/bin/$program" "./crossbeam/$program" 2>&1); then
		echo "$build_err" >&2
		die_on_build_error "the benchmark module" "$program" "$program" "$build_err"
	fi
done

messages=$(cd "$repo/benchmark" && "${go_env[@]}" go doc ./crossbeam/workloads Messages |
	awk '$1 == "const" && $2 == "Messages" && $3 == "=" { gsub(/_/, "", $4); print $4 }') ||
	die "go doc could not read workloads.Messages"
case "$messages" in
'' | *[!0-9]* | 0*)
	die "could not read a positive integer from workloads.Messages, got '$messages'"
	;;
esac
rust_messages=$(awk '
	/^pub const MESSAGES: usize = [0-9_]+;$/ {
		value = $6
		sub(/;$/, "", value)
		gsub(/_/, "", value)
		print value
		exit
	}
' "$crate/src/lib.rs") || die "awk could not read $crate/src/lib.rs"
[ "$rust_messages" = "$messages" ] ||
	die "MESSAGES in $crate/src/lib.rs is '$rust_messages', workloads.Messages is $messages"

status="aborted at the build checks"
# Accepts only the codegen options cargo's release profile produces, each exactly once, because
# cargo appends a config's build.rustflags after its own flags and rustc takes the last value of
# a repeated option, so merely finding the expected flags proves nothing. Requiring the
# sysroot's rustc as every line's program catches a rustc-wrapper or a swapped compiler from any
# source.
sysroot=$(cd "$crate" && rustc --print sysroot) || die "rustc --print sysroot failed in $crate"
expected_rustc="$sysroot/bin/rustc"
rustc_lines=$(grep -F 'Running `' "$cargo_build_log" | grep -F -- '--crate-name' || true)
build_problem=$(printf '%s\n' "$rustc_lines" | awk -v rustc="$expected_rustc" '
	# An entry ending in "=" accepts any value.
	BEGIN {
		allowed_count = split("codegen-units=16 embed-bitcode=no extra-filename= metadata= " \
			"opt-level=3 strip=debuginfo", allowed, " ")
	}
	function allowed_index(option,    a) {
		for (a = 1; a <= allowed_count; a++) {
			if (allowed[a] ~ /=$/ && index(option, allowed[a]) == 1) return a
			if (option == allowed[a]) return a
		}
		return 0
	}
	NF == 0 { next }
	{
		command = substr($0, index($0, "Running `") + length("Running `"))
		sub(/`$/, "", command)
		if (index(command, rustc " ") != 1) {
			program = command
			crate_name_at = index(command, " --crate-name")
			if (crate_name_at > 0) program = substr(command, 1, crate_name_at - 1)
			problem = "a rustc command line runs " program ", want " rustc
			exit
		}
		word_count = split(substr(command, length(rustc) + 2), word, " ")
	}
	/--crate-name build_script_build / { next }
	{
		crate_lines++
		delete seen
		for (w = 1; w <= word_count; w++) {
			if (word[w] == "-O") {
				problem = "a crate rustc command line sets -O"
				exit
			}
			if (word[w] == "-C" || word[w] == "--codegen") option = word[++w]
			else if (word[w] ~ /^-C./) option = substr(word[w], 3)
			else if (word[w] ~ /^--codegen=/) option = substr(word[w], length("--codegen=") + 1)
			else continue
			a = allowed_index(option)
			if (a == 0) {
				problem = "a crate rustc command line sets unexpected codegen option " option
				exit
			}
			if (a in seen) {
				problem = "a crate rustc command line repeats codegen option " option
				exit
			}
			seen[a] = 1
		}
		for (a = 1; a <= allowed_count; a++) {
			if (!(a in seen)) {
				problem = "a crate rustc command line lacks codegen option " allowed[a]
				exit
			}
		}
	}
	END {
		if (problem == "" && crate_lines == 0) problem = "no crate rustc command line"
		if (problem != "") print problem
	}
')
[ -z "$build_problem" ] || die "$build_problem, see $cargo_build_log"
for program in gochan ring; do
	bad_setting=$(go version -m "$results/bin/$program" | awk -F'\t' '
		$2 != "build" { next }
		$3 ~ /^GOEXPERIMENT=/ { print $3; exit }
		$3 ~ /^GOAMD64=/ && $3 != "GOAMD64=v1" { print $3; exit }
		$3 ~ /^GOARM64=/ && $3 != "GOARM64=v8.0" { print $3; exit }
	') || die "go version -m failed on $results/bin/$program"
	[ -z "$bad_setting" ] || die "$results/bin/$program was built with $bad_setting"
done

status="aborted at the tests"
echo "testing" >&2
if ! test_output=$(cd "$crate" && cargo test --frozen --target-dir "$results/target" 2>&1); then
	echo "$test_output" >&2
	die "the Rust tests failed, see the output above"
fi
if ! test_output=$(cd "$repo/benchmark" &&
	"${go_env[@]}" go test -tags crossbeamcheck -race -timeout 8m ./crossbeam/... 2>&1); then
	echo "$test_output" >&2
	die "the Go tests failed, see the output above"
fi
build_elapsed=$((SECONDS - build_start))

# State.
crossbeam_versions() {
	awk '
		$1 == "name" { name = $3; gsub(/"/, "", name) }
		$1 == "version" && name ~ /^crossbeam/ && name != "crossbeam-bench" {
			version = $3
			gsub(/"/, "", version)
			print name " " version
			name = ""
		}
	' "$crate/Cargo.lock"
}

# rustc and cargo run in the crate, so rustup picks the toolchain rust-toolchain.toml pins.
rustc_info() {
	(cd "$crate" && rustc -Vv)
}

cargo_info() {
	(cd "$crate" && cargo -V)
}

# Untracked files outside benchmark/ and scripts/, such as design/ and plans/, cannot change the
# build, as in ring-compare-bench.sh.
tree_status() {
	local tracked
	local untracked

	tracked=$(git -C "$repo" status --porcelain --untracked-files=no) || return
	untracked=$(git -C "$repo" status --porcelain --untracked-files=all -- benchmark/ scripts/ |
		awk '$1 == "??"') || return
	if [ -z "$tracked" ] && [ -z "$untracked" ]; then
		echo clean
		return
	fi
	echo dirty
	if [ -n "$tracked" ]; then
		printf '%s\n' "$tracked"
	fi
	if [ -n "$untracked" ]; then
		printf '%s\n' "$untracked"
	fi
}

write_state() {
	local program

	echo "## run"
	echo "command line: $command_line"
	echo "seed: $seed"
	echo "rounds: $rounds"
	echo "timeout: ${invocation_limit}s"
	echo "messages: $messages"
	record commit git -C "$repo" rev-parse HEAD
	record tree tree_status

	echo "## machine"
	record "cpu model" cpu_model
	echo "pin set: ${pin_cpus:-none, macOS cannot pin}"
	if [ "$platform" = Darwin ]; then
		record hw.ncpu sysctl -n hw.ncpu
		record hw.perflevel0 sysctl hw.perflevel0.physicalcpu hw.perflevel0.logicalcpu
		record hw.perflevel1 sysctl hw.perflevel1.physicalcpu hw.perflevel1.logicalcpu
	else
		record "transparent huge pages enabled" \
			cat /sys/kernel/mm/transparent_hugepage/enabled
		record "transparent huge pages defrag" cat /sys/kernel/mm/transparent_hugepage/defrag
	fi
	record "load average before" load_average

	echo "## toolchains"
	record "go version" go version
	record "rustc -Vv" rustc_info
	record "cargo -V" cargo_info
	record crossbeam crossbeam_versions
	record benchstat benchstat_module
	for program in gochan ring; do
		record "$program runtime.sched offset" sched_offset "$results/bin/$program"
	done

	for program in gochan ring; do
		echo "## go version -m $program"
		go version -m "$results/bin/$program"
	done

	echo "## rustc command lines"
	printf '%s\n' "$rustc_lines"
}

status="aborted writing the state"
write_state >>"$state"

# Rounds.
findings=()
# One "<kind> <workload> kept <secs>" or "<kind> <workload> rejected" line per sample.
outcomes=()
mkdir "$results/rounds"

# Prints whether out holds exactly one line "<workload> <Lang> <impl> <decimal number> sec".
is_result_line() {
	local out=$1
	local workload=$2
	local lang=$3
	local impl=$4

	awk -v workload="$workload" -v lang="$lang" -v impl="$impl" '
		NR == 1 {
			ok = NF == 5 && $1 == workload && $2 == lang && $3 == impl &&
				$4 ~ /^[0-9]+(\.[0-9]+)?$/ && $5 == "sec"
		}
		END { exit !(NR == 1 && ok) }
	' "$out"
}

run_invocation() {
	local kind=$1
	local workload=$2
	local round=$3
	local stem="$results/rounds/$round-$kind-$workload"
	local out="$stem.txt"
	local err="$stem.err"
	local marker="$stem.timed-out"
	local lang=${kind%%-*}
	local impl=${kind#*-}
	local exit_status=0
	local command=()
	local arg
	local first_signal
	local label="round $round $kind $workload"

	while IFS= read -r arg; do
		command+=("$arg")
	done < <(invocation_command "$kind" "$workload")
	case "$lang" in
	Go) first_signal=QUIT ;;
	*) first_signal=TERM ;;
	esac

	# Runs from the results directory, so anything the binary writes stays with the results. The
	# exec makes $! the binary's pid, or taskset's, which execs the binary under the same pid.
	(cd "$results" && exec ${pin[@]+"${pin[@]}"} "${command[@]}") >"$out" 2>"$err" &
	invocation_pid=$!
	watchdog "$invocation_pid" "$marker" "$invocation_limit" "$first_signal" &
	watchdog_pid=$!
	wait "$invocation_pid" || exit_status=$?
	invocation_pid=""
	kill "$watchdog_pid" 2>/dev/null || true
	wait "$watchdog_pid" 2>/dev/null || true
	watchdog_pid=""

	if [ -e "$marker" ]; then
		add_finding "$label timed out after ${invocation_limit}s, sent SIG$first_signal,\
 exit status $exit_status, output in $out and $err"
	elif [ "$exit_status" -ne 0 ]; then
		add_finding "$label exited with status $exit_status, output in $out and $err"
	elif [ -s "$err" ]; then
		add_finding "$label wrote to stderr, see $err"
	elif ! is_result_line "$out" "$workload" "$lang" "$impl"; then
		add_finding "$label did not write one result line, see $out"
	else
		outcomes+=("$kind $workload kept $(awk '{ print $4 }' "$out")")
		return
	fi
	outcomes+=("$kind $workload rejected")
}

rounds_start=$SECONDS
for ((k = 1; k <= rounds; k++)); do
	status="aborted at round $k"
	echo "round $k/$rounds" >&2
	while read -r kind workload; do
		run_invocation "$kind" "$workload" "$k"
	done < <(expand_invocations | shuffle $((seed + k)))
	if [ "$k" -eq 1 ]; then
		first_round_elapsed=$((SECONDS - rounds_start))
		if [ "$rounds" -gt 1 ]; then
			echo "round 1 took ${first_round_elapsed}s," \
				"about $((first_round_elapsed * (rounds - 1)))s remain" >&2
		fi
	fi
done
rounds_elapsed=$((SECONDS - rounds_start))

# Summary.
status="aborted at the summary"

# Prints one "<kind> <workload> <kept> <rejected>" line per expected cell, in table order.
cell_counts() {
	{
		expand_invocations | awk '{ print "expected", $0 }'
		if [ ${#outcomes[@]} -gt 0 ]; then
			printf '%s\n' "${outcomes[@]}"
		fi
	} | awk '
		$1 == "expected" {
			cell = $2 " " $3
			order[++count] = cell
			next
		}
		$3 == "kept" { kept[$1 " " $2]++ }
		$3 == "rejected" { rejected[$1 " " $2]++ }
		END {
			for (i = 1; i <= count; i++) {
				print order[i], kept[order[i]] + 0, rejected[order[i]] + 0
			}
		}
	'
}

incomplete_cells() {
	cell_counts | awk -v min="$min_samples" '$3 < min { print $1, $2 }'
}

excluded_cells() {
	cell_counts | awk -v min="$min_samples" '$3 >= min && $4 > 0 { print $1, $2 }'
}

kept_samples() {
	if [ ${#outcomes[@]} -gt 0 ]; then
		printf '%s\n' "${outcomes[@]}" | awk '$3 == "kept"'
	fi
}

# Prints each kept sample as a Go benchmark line in nanoseconds per message.
write_samples() {
	kept_samples | awk -v messages="$messages" '
		{
			split($2, workload, "_")
			printf "BenchmarkSuite/cap=%s/topo=%s/impl=%s %d %.4f ns/op\n",
				workload[1], workload[2], $1, messages, $4 * 1e9 / messages
		}
	'
}

# Prints each cell's median in the programs' line format. An even count takes the mean of the two
# middle values, as benchstat does.
write_medians() {
	local kind
	local workload

	while read -r kind workload; do
		kept_samples | awk -v kind="$kind" -v workload="$workload" \
			'$1 == kind && $2 == workload { print $4 }' | LC_ALL=C sort -g |
			awk -v kind="$kind" -v workload="$workload" '
				{ value[NR] = $1 }
				END {
					if (NR == 0) exit
					if (NR % 2 == 1) {
						median = value[(NR + 1) / 2]
					} else {
						median = (value[NR / 2] + value[NR / 2 + 1]) / 2
					}
					lang = kind
					sub(/-.*/, "", lang)
					impl = kind
					sub(/^[^-]*-/, "", impl)
					printf "%-25s %-15s %.6f sec\n", workload, lang " " impl, median
				}
			'
	done < <(expand_invocations)
}

# Prints "<prefix> <topology> <impl A> <impl B> <faster impl>" for each pair that the CSV marks
# significant against its base column, leaving out pairs with a cell listed in bad_file.
significant_pairs() {
	local csv=$1
	local bad_file=$2

	awk -F, -v bad_file="$bad_file" '
		BEGIN {
			while ((getline line < bad_file) > 0) {
				bad[line] = 1
			}
		}
		# The first header row names each implementation at its first sub-column. The base
		# column has sec/op and CI, and every other has sec/op, CI, vs base, and P.
		FNR == 1 {
			for (i = 2; i <= NF; i++) {
				if ($i != "") name[i] = $i
			}
			base = name[2]
			next
		}
		FNR == 2 || $1 == "geomean" || $1 == "" { next }
		{
			split($1, row, " ")
			workload = row[1] "_" row[2]
			if ((base " " workload) in bad) next
			for (i in name) {
				if (i == 2) continue
				other = name[i]
				change = $(i + 2)
				if (change !~ /^[+-]/) continue
				if ((other " " workload) in bad) continue
				faster = change ~ /^\+/ ? base : other
				if (base < other) {
					print row[1], row[2], base, other, faster
				} else {
					print row[1], row[2], other, base, faster
				}
			}
		}
	' "$csv"
}

write_outcome_sections() {
	local incomplete
	local excluded

	incomplete=$(incomplete_cells)
	excluded=$(excluded_cells)
	echo "## findings"
	if [ ${#findings[@]} -eq 0 ]; then
		echo "none"
	else
		printf '%s\n' "${findings[@]}"
	fi
	echo "## incomplete cells"
	echo "${incomplete:-none}"
	echo "## excluded cells"
	echo "${excluded:-none}"
	echo "## timing"
	echo "build and test elapsed: ${build_elapsed}s"
	echo "round 1 elapsed: ${first_round_elapsed}s"
	echo "rounds elapsed: ${rounds_elapsed}s"
	echo "total elapsed: ${SECONDS}s"
	record "load average after" load_average
}

if [ -z "$(kept_samples)" ]; then
	write_outcome_sections >>"$state"
	die "no invocation succeeded, see the findings in $state"
fi

write_samples >"$results/samples.txt"
write_medians >"$results/median.txt"

scratch=$(mktemp -d)
: >"$scratch/pairs.txt"
{
	incomplete_cells
	excluded_cells
} >"$scratch/bad-cells.txt"

# Runs benchstat in the results directory with the given arguments, writing its output to out
# and its full stderr to err. benchstat exits 0 when it cannot open its input or rejects its
# arguments, printing the error on stderr, so the exit status alone does not show a failure.
# Its per-cell warnings, such as "B3: need >= 6 samples ...", are normal and also go to stderr.
# The call fails on a non-zero exit status, no output, or a stderr line that is not a cell
# warning.
run_benchstat() {
	local out=$1
	local err=$2
	shift 2

	(cd "$results" && benchstat "$@") >"$out" 2>"$err" || return
	[ -s "$out" ] && ! grep -qvE '^[A-Z]+[0-9]+: ' "$err"
}

while read -r kind; do
	columns=$(summary_columns "$kind")
	args=(-row "$summary_rows" -col "$columns" samples.txt)
	text_err="$results/benchstat-$kind-text.err"
	if ! run_benchstat "$results/summary-$kind.txt" "$text_err" "${args[@]}"; then
		add_finding "benchstat failed for summary-$kind.txt, see $text_err"
		continue
	fi
	csv_err="$results/benchstat-$kind.err"
	if ! run_benchstat "$scratch/summary-$kind.csv" "$csv_err" -format csv "${args[@]}"; then
		add_finding "benchstat -format csv failed for $kind, see $csv_err"
		continue
	fi
	significant_pairs "$scratch/summary-$kind.csv" "$scratch/bad-cells.txt" >>"$scratch/pairs.txt"
done < <(go_kinds)
# A Go-vs-Go pair appears in both kinds' summaries.
LC_ALL=C sort -u "$scratch/pairs.txt" >"$results/significant.txt"

if command -v uv >/dev/null 2>&1; then
	(cd "$repo" && uv run --project benchmark --locked python benchmark/crossbeam/plot.py \
		"$results/median.txt") || warn "plotting failed, see the output above"
fi

# Exit.
write_outcome_sections >>"$state"
incomplete=$(incomplete_cells)
excluded=$(excluded_cells)
for kind in $(go_kinds); do
	cat "$results/summary-$kind.txt"
	echo
done
if [ ${#findings[@]} -gt 0 ]; then
	echo "findings:"
	printf '%s\n' "${findings[@]}"
fi
if [ -n "$incomplete" ]; then
	echo "incomplete cells:"
	printf '%s\n' "$incomplete"
fi
if [ -n "$excluded" ]; then
	echo "excluded cells:"
	printf '%s\n' "$excluded"
fi
echo "results: $results"
echo "rounds elapsed: ${rounds_elapsed}s"
echo "total elapsed: ${SECONDS}s"
if [ ${#findings[@]} -gt 0 ] || [ -n "$incomplete" ]; then
	status="complete with findings"
	exit 1
fi
status=complete
