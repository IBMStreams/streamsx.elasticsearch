#--variantList='NotStartOfCR NotSupportedCheckpointOperatorPeriodic NotSupportedCheckpointOperatorDriven'

setCategory 'quick'
PREPS='copyAndMorphSpl'
STEPS=(
	'splCompileInterceptAndError'
	'myEval'
)

myEval() {
	case "$TTRO_variantCase" in
	NotStartOfCR)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" 'ERROR: CDIST3500E*';;
	NotSupportedCheckpointOperatorPeriodic)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" 'ERROR: CDIST3501E*';;
	NotSupportedCheckpointOperatorDriven)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" 'ERROR: CDIST3502E*';;
	*)
		printErrorAndExit "Wrong case variant" $errRt
	esac
}
