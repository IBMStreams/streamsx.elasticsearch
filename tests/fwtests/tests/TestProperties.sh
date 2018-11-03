# samples path
setVar 'TTRO_streamsxEsSamplesPath' "$TTRO_inputDir/../../../samples"

# path to toolkit under test
setVar 'TTPR_streamsxEsToolkit' "$TTRO_inputDir/../../../com.ibm.streamsx.elasticsearch"

# add other needed toolkit pathes here, if any
#setVar 'TTPR_streamsxJsonToolkit' "$STREAMS_INSTALL/toolkits/com.ibm.streamsx.json"

# toolkit path used for compilation (-t)
setVar 'TT_toolkitPath' "${TTPR_streamsxEsToolkit}"
#setVar 'TT_toolkitPath' "${TTPR_streamsxEsToolkit}:${TTPR_streamsxJsonToolkit}" #consider more than one tk...

# set directory where the server install/run scripts are located
setVar 'TT_serverDir' "$TTRO_inputDir/../../setup"
# set ES version to test against
setVar 'TT_serverVersion' "6.2.2"

#add timeouts if needed
#setVar 'TTPR_waitForJobHealth' 120
#setVar 'TTPR_timeout' 600
