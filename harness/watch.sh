#!/bin/sh
# Watch a detached `prove.py all` without getting swept up in its teardown.
#
# The harness kills any host process that holds a file under results/ open,
# names the project directory on its command line, or looks like a loop around
# prove.py. That is deliberate -- four runs left a sampler running while the
# teardown check passed honestly -- but it also kills the shell someone is
# waiting in. Clean-room run 44 lost one by appending `cat .../PROGRESS.txt` to
# the same command line as something else, and it is at least the second run to
# lose one this way.
#
# This script keeps the project directory out of its own command line: the path
# arrives in an environment variable, which the sweep does not read.
#
#   PROJECT_DIR=/path/to/run sh harness/watch.sh
#
# It prints progress until results/DONE appears, then prints the outcome and
# exits 0 if the run passed, 1 otherwise. With WATCH_MINUTES set it gives up
# waiting after that many minutes and exits 3, saying the chain is still
# running: an agent whose tool cannot wait two hours in one command runs it
# again and again, in the foreground, until it exits 0 or 1. Ending a turn to
# wait is what ended clean-room run 54's agent before the suite finished. Every line also goes to watch.log in
# the run's directory, outside results/: an agent's tool may keep this script's
# output somewhere the agent is not allowed to read (clean-room run 54, F5).
#
# One small exposure is left and is not worth more machinery: the sweep also
# kills anything holding a file open under results/, and this script has
# PROGRESS.txt open for the instant it reads it, once every 20 seconds. The
# sweep runs twice in a run -- at chain start and at teardown -- so the two
# have to collide inside that instant. If it ever happens, start the script
# again; nothing about the run is affected.

if [ -z "$PROJECT_DIR" ]; then
    echo "set PROJECT_DIR to the run's directory, then run this again:"
    echo "  PROJECT_DIR=/path/to/run sh harness/watch.sh"
    exit 2
fi
if [ ! -d "$PROJECT_DIR" ]; then
    echo "no such directory in PROJECT_DIR"
    exit 2
fi

R="$PROJECT_DIR/results"
LOG="$PROJECT_DIR/watch.log"
say() { printf '%s\n' "$1"; printf '%s\n' "$1" >> "$LOG"; }
case "${WATCH_MINUTES:-}" in
    ''|*[!0-9]*) [ -n "${WATCH_MINUTES:-}" ] && { echo "WATCH_MINUTES must be a whole number of minutes"; exit 2; }
                 left=-1 ;;
    *)           left=$((WATCH_MINUTES * 3)) ;;
esac
last=""
while [ ! -f "$R/DONE" ]; do
    if [ "$left" -eq 0 ]; then
        say "$(date '+%H:%M:%S')  still running after $WATCH_MINUTES minutes, no results/DONE yet: run this again to keep waiting"
        exit 3
    fi
    [ "$left" -gt 0 ] && left=$((left - 1))
    if [ -f "$R/PROGRESS.txt" ]; then
        now=$(cat "$R/PROGRESS.txt" 2>/dev/null)
        if [ "$now" != "$last" ]; then
            say "$(date '+%H:%M:%S')  $now"
            last="$now"
        fi
    fi
    sleep 20
done

say ""
say "finished: $(cat "$R/DONE")"
case "$(cat "$R/DONE")" in
    PASS*) exit 0 ;;
    *)     exit 1 ;;
esac
