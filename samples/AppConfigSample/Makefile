# Copyright (C)2018, International Business Machines Corporation and  
# others. All Rights Reserved.                        

.PHONY: all clean

# determine pathes
ifeq ($(STREAMS_STUDIO_BUILDING), 1)
  # Building from Streams Studio, use env vars set by studio
  SPLC = $(STREAMS_STUDIO_SC_PATH)
  SPL_PATH = $(STREAMS_STUDIO_SPL_PATH)
  OUTPUT_DIR = $(STREAMS_STUDIO_OUTPUT_DIRECTORY)
else
  # Building outside of Streams Studio...
  ifeq ($(STREAMS_INSTALL),)
    $(error error: environment variable STREAMS_INSTALL has to be set)
  endif
  # Set the ELASTICSEARCH_TOOLKIT_HOME variable if the toolkit or sample location changed
  ifeq ($(ELASTICSEARCH_TOOLKIT_HOME),)
    ifneq ($(realpath ../../com.ibm.streamsx.elasticsearch),)
      ELASTICSEARCH_TOOLKIT_HOME = ../../com.ibm.streamsx.elasticsearch
    endif
    ifneq ($(realpath $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.elasticsearch),)
      ELASTICSEARCH_TOOLKIT_HOME = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.elasticsearch
    endif
  endif
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH = $(ELASTICSEARCH_TOOLKIT_HOME)
endif

# directories and compiler options
DATA_DIR = /tmp
SPL_NAMESPACE = com.ibm.streamsx.elasticsearch.sample.AppConfig
SPL_MAIN_COMPOSITE = $(SPL_NAMESPACE)::Main
SPLC_FLAGS  = -a -t $(SPL_PATH) --data-directory=$(DATA_DIR)
SPL_CMD_ARGS ?= 
OUTPUT_DIR = output

all:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-directory $(OUTPUT_DIR) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-directory $(OUTPUT_DIR)
	-rm -rf $(OUTPUT_DIR)
	-rm -rf toolkit.xml
	-rm -rf .toolkitList

configure:
	-streamtool rmappconfig --noprompt es
	streamtool mkappconfig --property userName=$(ES_USER) --property password=$(ES_PASSWORD) --property nodeList=$(ES_NODES) --property sslEnabled=true es


