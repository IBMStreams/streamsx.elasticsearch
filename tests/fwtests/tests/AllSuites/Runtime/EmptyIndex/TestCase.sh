# Submission test for Streams
##--variantList=''

setCategory 'quick'
PREPS='copyOnly splCompile'
STEPS='es_runStandalone Evaluate'
#FINS=

Evaluate() {
	# check for the error on empty index attr
	if ! egrep "CDIST3507E Unknown index" standalone.log
	then
		setFailure "Pattern not found in log" 
	fi
	return 0
}

