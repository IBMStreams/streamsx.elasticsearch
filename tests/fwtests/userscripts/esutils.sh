####################################################
# elasticsearch test suite utilities

####################################################
# Initialization section

# Module initialization goes here

####################################################
# Functions section

TTRO_help_es_ensureDomainStarted='
# Function es_ensureDomainStarted
#	Check if the domain specified in the TTPRN_streamsDomainId variable is running.
#   If the domain is not running, try to start it.'
function es_ensureDomainStarted {
	isDebug && printDebug "$FUNCNAME $*"

	if ! echoAndExecute $TTPRN_st lsdomain --started "$TTPRN_streamsDomainId" ; then
		printInfo "$FUNCNAME : Domain $TTPRN_streamsDomainId is not started, trying to start ..."
		if ! echoAndExecute $TTPRN_st startdomain -d "$TTPRN_streamsDomainId" ; then
			printError "$FUNCNAME : Domain $TTPRN_streamsDomainId could not be started."
			return $errTestFail
		fi
		printInfo "$FUNCNAME : Domain $TTPRN_streamsDomainId restarted successfully."
	fi
	return 0
}
export -f es_ensureDomainStarted

TTRO_help_es_ensureInstanceStarted='
# Function es_ensureInstanceStarted
#	Check if the instance specified in the TTPRN_streamsInstanceId variable is running
#   in the domain specified in TTPRN_streamsDomainId variable.
#   If the instance is not running, try to start it.'
function es_ensureInstanceStarted {
	isDebug && printDebug "$FUNCNAME $*"

	echo "$TTPRN_st lsinstance -d $TTPRN_streamsDomainId --started $TTPRN_streamsInstanceId" 
	inst=`$TTPRN_st lsinstance -d "$TTPRN_streamsDomainId" --started "$TTPRN_streamsInstanceId"`
	if [ "$inst" != "$TTPRN_streamsInstanceId" ] ; then
		printInfo "$FUNCNAME : Instance $TTPRN_streamsInstanceId is not started, trying to start ..."
		if ! echoAndExecute $TTPRN_st startinstance -d "$TTPRN_streamsDomainId" -i "$TTPRN_streamsInstanceId" ; then
			printError "$FUNCNAME : Instance $TTPRN_streamsInstanceId could not be started."
			return $errTestFail
		fi
		printInfo "$FUNCNAME : Instance $TTPRN_streamsInstanceId restarted successfully."
	fi
	return 0
}
export -f es_ensureInstanceStarted

TTRO_help_es_installServer='
# Function es_installServer
#	Install the elasticsearch server'
function es_installServer {
	isDebug && printDebug "$FUNCNAME $*"

	currDir=`pwd`
	if ! echoAndExecute cd $TT_serverDir ; then
		printError "Server install directory $TT_serverDir does not exists, cannot cd to it."
		return $errTestFail
	fi
	if ! echoAndExecute ./installServer.sh $TT_serverVersion ; then
		printError "Installing server version failed."
		return $errTestFail
	fi
	cd $currDir
	return 0
}
export -f es_installServer

TTRO_help_es_cleanServer='
# Function es_cleanServer
#	Clean the elasticsearch server install directories'
function es_cleanServer {
	isDebug && printDebug "$FUNCNAME $*"

	currDir=`pwd`
	if ! echoAndExecute cd $TT_serverDir ; then
		printError "Server install directory $TT_serverDir does not exists, cannot cd to it."
		return $errTestFail
	fi
	if ! echoAndExecute ./cleanServer.sh $TT_serverVersion ; then
		printError "Cleaning server directory failed."
		return $errTestFail
	fi
	cd $currDir
	return 0
}
export -f es_cleanServer

TTRO_help_es_startNode='
# Function es_startNode
#	Start one ES node'
function es_startNode {
	isDebug && printDebug "$FUNCNAME $*"

	currDir=`pwd`
	if ! echoAndExecute cd $TT_serverDir ; then
		printError "Server install directory $TT_serverDir does not exists, cannot cd to it."
		return $errTestFail
	fi
	if ! echoAndExecute ./runNodeDaemon.sh 1 $TT_serverVersion ; then
		printError "Starting server node failed."
		return $errTestFail
	fi
	cd $currDir
	return 0
}
export -f es_startNode

TTRO_help_es_stopNodee='
# Function es_stopNode
#	Stop one ES node'
function es_stopNode {
	isDebug && printDebug "$FUNCNAME $*"

	currDir=`pwd`
	if ! echoAndExecute cd $TT_serverDir ; then
		printError "Server install directory $TT_serverDir does not exists, cannot cd to it."
		return $errTestFail
	fi
	if ! echoAndExecute ./stopNode.sh 1 $TT_serverVersion ; then
		printError "Stoping server node failed."
		return $errTestFail
	fi
	cd $currDir
	return 0
}
export -f es_stopNode


TTRO_help_es_dumpIndex='
# Function es_dumpIndex
#	Dump an index to a file, using these parameters
#   $1 the name of the index
#   $2 the number of documents to dump
#   $3 a number field in the document to sort the output by'
function es_dumpIndex {
	isDebug && printDebug "$FUNCNAME $*"

	qurl="localhost:9200"
	qstring="${qurl}/$1/_search?q=*&pretty&size=$2&sort=$3"
	printInfo "Dump index string: $qstring"
	
	curl -s -S -X GET "$qstring" >${1}.dump
	rc=$?
	if [ $rc != 0 ] ; then
		printError "Cannot dump index $1 : $rc"
		return $errTestFail
	fi
	return 0
}
export -f es_dumpIndex

TTRO_help_es_maskIndexDocFields='
# Function es_maskIndexDocFields
#	Set the content of the given fields to a certain string, to simplify comparison of fields
#   Use this for timestamp strings for example, which may differ from testrun to testrun
#   $1 the name of the index, the same index file in the expected directory is also masked
#   all remaining parameters are the field to mask'
function es_maskIndexDocFields {
	isDebug && printDebug "$FUNCNAME $*"

	idxFile=${1}.dump
	idxRefFile=expected/${1}.dump
	
	for f in "${@:2}"
	do
		perl -i -npe 's/^(\s*\"'$f'\"\s*:\s*\").*(\"\s*)$/$1XXX$2/' $idxFile
		perl -i -npe 's/^(\s*\"'$f'\"\s*:\s*\").*(\"\s*)$/$1XXX$2/' $idxRefFile
	done

	return 0
}
export -f es_maskIndexDocFields

TTRO_help_es_matchIndexDocFields='
# Function es_matchIndexDocFields
#   Match fields from an expected file against fields dumped from the index after a test.
#   The expected index dump needs to be stored in file <indexname>.dump_exp
#   The actual dump needs to be stored in file <indexname>.dump
#   Parameters :
#   $1 the index name
#   all remaining parameters are the field names to match against'
function es_matchIndexDocFields {
	isDebug && printDebug "$FUNCNAME $*"

	idxFile=${1}.dump
	idxRefFile=expected/${1}.dump
	
	grepList=""
	isFirst=1
	for f in "${@:2}"
	do
		if [ $isFirst != 1 ] ; then
			grepList=${grepList}"|"
		fi
		grepList=${grepList}"\""$f
		isFirst=0
	done
	printInfo "Grepstring: $grepList"

	if [ ! -f $idxFile ] ; then 
		printError "Cannot read file $idxFile"
		return $errTestFail
	fi
	egrep $grepList $idxFile >$idxFile.extracted

	if [ ! -f $idxRefFile ] ; then 
		printError "Cannot read file $idxRefFile"
		return $errTestFail
	fi
	egrep $grepList $idxRefFile >$idxRefFile.extracted
	
	if ! diff $idxFile.extracted $idxRefFile.extracted ; then
		printError "Fields do not match"
		return $errTestFail
	fi
	return 0
}
export -f es_matchIndexDocFields

TTRO_help_es_dropIndex='
# Function es_dropIndex
#	Delete an index from the server
#   $1 the name of the index'
function es_dropIndex {
	isDebug && printDebug "$FUNCNAME $*"

	qurl="localhost:9200"
	qstring="${qurl}/$1"
	printInfo "Drop index string: $qstring"
	
	curl -s -S -X DELETE "$qstring"
	rc=$?
	if [ $rc != 0 ] ; then
		printError "Cannot delete index $1 : $rc"
		return $errTestFail
	fi
	return 0
}
export -f es_dropIndex

TTRO_help_es_runStandalone='
# Function es_runStandalone
#	Run the application as standalone
#   invokes output/bin/standalone and redirects stdout and stderr to standalone.log'
function es_runStandalone {
	isDebug && printDebug "$FUNCNAME $*"

	# create data dir, if not present. it is needed by the testfilesink
	if [ ! -d data ] ; then
		mkdir data
	fi
	executeAndLog ./output/bin/standalone
	echo resultcode: $TTTT_result
	mv $TT_evaluationFile standalone.log
	return 0
}
export -f es_runStandalone

