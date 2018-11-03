# Submission test for Streams
##--variantList=''

setCategory 'quick'
PREPS='copyOnly splCompile'
STEPS='es_runStandalone Evaluate'
#FINS=

Evaluate() {
	# check for the error on empty index attr
	if ! egrep "CDIST3507E Unknown index. The specified index string is either 'null' or empty. Input tuple is ignored." standalone.log
	then
		setFailure "Pattern not found in log" 
	fi
	return 0
}

