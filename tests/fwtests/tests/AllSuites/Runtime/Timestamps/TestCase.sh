# Submission test for Streams
##--variantList=''

PREPS='copyOnly splCompile'
STEPS='submitJob checkJobNo waitForFin cancelJob Evaluate'
FINS=(cancelJob 'es_dropIndex idx1' 'es_dropIndex idx2' 'es_dropIndex idx3')

Evaluate() {
	if ! echoAndExecute es_dumpIndex idx1 3 id ; then
		setFailure 'Cannot dump index'
	fi
	if ! echoAndExecute es_dumpIndex idx2 3 id ; then
		setFailure 'Cannot dump index'
	fi
	if ! echoAndExecute es_dumpIndex idx3 3 id ; then
		setFailure 'Cannot dump index'
	fi

# TODO mask timestamp field content to avoid timezone issues 

	if ! echoAndExecute es_matchIndexDocFields idx1 _index _type tstamp rmsg id ; then
		setFailure 'Match document fields in idx1 failed'
	fi
	if ! echoAndExecute es_matchIndexDocFields idx2 _index _type tstamp rmsg id ; then
		setFailure 'Match document fields in idx2 failed'
	fi
	if ! echoAndExecute es_matchIndexDocFields idx3 _index _type rmsg id tupleTime ; then
		setFailure 'Match document fields in idx3 failed'
	fi

	return 0
}
