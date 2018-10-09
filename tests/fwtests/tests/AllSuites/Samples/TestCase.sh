##--variantList="$TTRO_streamsxEsSamples"

setCategory 'quick'

# TODO use proper variant list after renaming the sample (remove dots)

PREPS=(
	'export SPL_CMD_ARGS=""'
	'export STREAMSX_ES_TOOLKIT="$TT_toolkitPath"'
)

function testStep1 {
	local save="$PWD"
	cd "$TTRO_streamsxEsSamplesPath/com.ibm.streamsx.elasticsearch.sample.ECG"
	echoExecuteAndIntercept2 'success' 'make'
	cd "$save"
	return 0
}

STEPS="testStep1"
