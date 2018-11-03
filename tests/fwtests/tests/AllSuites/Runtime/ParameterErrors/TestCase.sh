# Submission test for Streams
#--variantList='ERR1 ERR2 ERR3 ERR4 ERR5 ERR6 ERR7'

setCategory 'quick'
PREPS='copyAndMorphSpl splCompile'
STEPS='es_runStandalone Evaluate'

Evaluate() {
	case "$TTRO_variantCase" in
	ERR1)
		egrep "CDIST3508E Parameter 'hostPort' has an invalid value of '-1'" standalone.log ;;
	ERR2)
		egrep "CDIST3508E Parameter 'nodeList' has an invalid value of 'aaa:bbb:ccc'" standalone.log ;;
	ERR3)
		egrep "CDIST3508E Parameter 'nodeList' has an invalid value of 'aaa:bbb'" standalone.log ;;
	ERR4)
		egrep "CDIST3508E Parameter 'reconnectionPolicyCount' has an invalid value of '-2'" standalone.log ;;
	ERR5)
		egrep "CDIST3508E Parameter 'readTimeout' has an invalid value of '-3'" standalone.log ;;
	ERR6)
		egrep "CDIST3508E Parameter 'connectionTimeout' has an invalid value of '-1'" standalone.log ;;
	ERR7)
		egrep "CDIST3508E Parameter 'maxConnectionIdleTime' has an invalid value of '-1'" standalone.log ;;
	*)
		printErrorAndExit "Wrong case variant" $errRt
	esac
	return 0
}

