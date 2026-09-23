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
# exits 0 if the run passed, 1 otherwise.
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
last=""
while [ ! -f "$R/DONE" ]; do
    if [ -f "$R/PROGRESS.txt" ]; then
        now=$(cat "$R/PROGRESS.txt" 2>/dev/null)
        if [ "$now" != "$last" ]; then
            printf '%s  %s\n' "$(date '+%H:%M:%S')" "$now"
            last="$now"
        fi
    fi
    sleep 20
done

echo
echo "finished: $(cat "$R/DONE")"
case "$(cat "$R/DONE")" in
    PASS*) exit 0 ;;
    *)     exit 1 ;;
esac
