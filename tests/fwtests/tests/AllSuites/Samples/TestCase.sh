#--variantList=$(\
#--for x in $TTRO_streamsxEsSamplesPath/*; \
#--	do if [[ -f $x/Makefile ]]; then \
#--		echo -n "${x#$TTRO_streamsxEsSamplesPath/} "; \
#--	fi; \
#--	done\
#--)

setCategory 'quick'

PREPS=(
	'copyAndMorph "$TTRO_streamsxEsSamplesPath/$TTRO_variantCase" "$TTRO_workDirCase" ""'
	'export SPL_CMD_ARGS=""'
	'export ELASTICSEARCH_TOOLKIT_HOME="$TTPR_streamsxEsToolkit"'
)

STEPS=( 'echoExecuteInterceptAndSuccess make' )
