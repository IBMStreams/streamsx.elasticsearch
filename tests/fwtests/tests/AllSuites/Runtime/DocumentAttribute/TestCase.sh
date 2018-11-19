# Submission test for Streams
##--variantList=''

setCategory 'quick'
PREPS='copyOnly splCompile'
STEPS='submitJob checkJobNo waitForFin cancelJobAndLog Evaluate'
FINS=(cancelJob 'es_dropIndex index1' )

Evaluate() {
	if ! echoAndExecute es_dumpIndex index1 5 id ; then
		setFailure 'Cannot dump index1'
	fi

	if ! echoAndExecute es_matchIndexDocFields index1 _index _type id rmsg umsg int32val int64val uint32val uint64val float32val float64val boolval ; then
		setFailure 'Match document fields in index1 failed'
	fi

	return 0
}
