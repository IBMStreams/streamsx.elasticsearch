# Submission test for Streams
#--variantList='List Set Map Tuple'

setCategory 'quick'
PREPS='copyAndMorphSpl splCompile'
STEPS='es_runStandalone Evaluate'

Evaluate() {
	case "$TTRO_variantCase" in
	List)
		egrep "CDIST3506E Input attribute 'intlist' has unsupported type: 'LIST'" standalone.log ;;
	Set)
		egrep "CDIST3506E Input attribute 'intset' has unsupported type: 'SET'" standalone.log ;;
	Map)
		egrep "CDIST3506E Input attribute 'intmap' has unsupported type: 'MAP'" standalone.log ;;
	Tuple)
		egrep "CDIST3506E Input attribute 'someTuple' has unsupported type: 'TUPLE'" standalone.log ;;
	*)
		printErrorAndExit "Wrong case variant" $errRt
	esac
	return 0
}

