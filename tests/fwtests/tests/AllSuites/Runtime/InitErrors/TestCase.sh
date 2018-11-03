# Submission test for Streams
#--variantList='NoPassword WrongTrustfile WrongTrustpwd'

setCategory 'quick'
PREPS='copyAndMorphSpl splCompile'
STEPS='copyCertFile es_runStandalone Evaluate delCertFile'

Evaluate() {
	case "$TTRO_variantCase" in
	NoPassword)
		egrep "Config error: userName for HTTP basic authentication is specified, nut no password is set." standalone.log ;
		egrep "CDIST3509E Invalid client configuration" standalone.log ;;
	WrongTrustfile)
		grep "Init error: cannot build SSLContext with truststore (no passwd), see stack trace for details" standalone.log ;
		egrep "java.io.FileNotFoundException" standalone.log ;
		egrep "CDIST3510E Client initialization failed" standalone.log ;;
	WrongTrustpwd)
		egrep "Init error: cannot build SSLContext with truststore and truststore password, see stack trace for details" standalone.log ;
		egrep "java.io.IOException" standalone.log ;
		egrep "CDIST3510E Client initialization failed" standalone.log ;;
	*)
		printErrorAndExit "Wrong case variant" $errRt
	esac
	return 0
}

copyCertFile() {
	cp cacerts /tmp
}

delCertFile() {
	rm /tmp/cacerts
}
