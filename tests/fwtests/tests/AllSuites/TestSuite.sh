# The common test suite for all elasticsearch tests

# import common test utils
import "$TTRO_scriptDir/streamsutils.sh"

#collect all samples as variant string for case Samples
all=''
short=''
cd "$TTRO_streamsxEsSamplesPath"
for x in $TTRO_streamsxEsSamplesPath/*; do
	if [[ -f $x/Makefile ]]; then
		short="${x#$TTRO_streamsxEsSamplesPath/}"
		all="$all $short"
	fi
done
printInfo "All samples are: $all"
setVar 'TTRO_streamsxEsSamples' "$all"

#PREPS=(
#	'export'
#	'ps'
#)