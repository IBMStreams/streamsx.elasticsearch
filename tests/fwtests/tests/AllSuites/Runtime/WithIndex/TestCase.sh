# Submission test for Streams
##--variantList=''

setCategory 'quick'
PREPS='copyOnly splCompile'
STEPS='submitJob checkJobNo waitForFin cancelJob Evaluate'
FINS=(cancelJob 'es_dropIndex index1' 'es_dropIndex index2')

Evaluate() {

	if ! echoAndExecute es_dumpIndex index1 5 id ; then
		setFailure 'Cannot dump index1'
	fi
	if ! echoAndExecute es_dumpIndex index2 5 id ; then
		setFailure 'Cannot dump index2'
	fi

	if ! echoAndExecute es_matchIndexDocFields index1 _index _type id ; then
		setFailure 'Match document fields in index1 failed'
	fi
	if ! echoAndExecute es_matchIndexDocFields index2 _index _type id ; then
		setFailure 'Match document fields in index2 failed'
	fi

	return 0
}

