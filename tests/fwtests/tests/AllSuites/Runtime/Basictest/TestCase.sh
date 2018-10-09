# Submission test for Streams
##--variantList='submitJob submitJobWithParam submitJobAndIntercept submitJobInterceptAndSuccess submitJobLogAndIntercept doubleJobCancel'
PREPS='copyOnly splCompile'
STEPS='submitJob checkJobNo waitForFin cancelJob myEvaluate'
FINS='cancelJob'

myEvaluate() {
	if ! linewisePatternMatch "$TT_dataDir/Tuples" '' '*http://httpbin.org/get*'; then
		setFailure 'No match found'
	fi
}

mySubmit() {
	case $TTRO_variantCase in
	submitJob)
		submitJob
		echo "-------- TTTT_jobno=$TTTT_jobno";;
#	submitJobWithParam)
#		submitJob -P 'aparam=bla bla'
#		echo "-------- TTTT_jobno=$TTTT_jobno";;
#	submitJobAndIntercept)
#		submitJobAndIntercept
#		echo "-------- TTTT_jobno=$TTTT_jobno"
#		echo "-------- TTTT_result=$TTTT_result";;
#	submitJobInterceptAndSuccess)
#		submitJobInterceptAndSuccess
#		echo "-------- TTTT_jobno=$TTTT_jobno"
#		echo "-------- TTTT_result=$TTTT_result";;
#	submitJobLogAndIntercept)
#		submitJobLogAndIntercept
#		echo "-------- TTTT_jobno=$TTTT_jobno"
#		echo "-------- TTTT_result=$TTTT_result"
#		echo "--------"
#		cat "$TT_evaluationFile";;
#	doubleJobCancel)
#		submitJobLogAndIntercept
#		echo "-------- TTTT_jobno=$TTTT_jobno"
#		echo "-------- TTTT_result=$TTTT_result"
#		cat "$TT_evaluationFile";;
	esac
}

myCancelJob() {

	cancelJob
	
#	if [[ $TTRO_variantCase == 'doubleJobCancel' ]]; then
#		cancelJob
#		echo "-------- TTTT_jobno=$TTTT_jobno"
#		echo "--------- and one more cancel job"
#		cancelJob
#	else
#		cancelJob
#	fi

}