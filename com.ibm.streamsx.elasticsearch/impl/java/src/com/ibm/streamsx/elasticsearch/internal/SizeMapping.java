//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch.internal;

import com.ibm.json.java.JSONObject;

import io.searchbox.action.GenericResultAbstractAction;

public class SizeMapping extends GenericResultAbstractAction {
	
	private static final String ENABLED_PROPERTY = "enabled";
	private static final String MAPPING_ATTR = "_mapping";
	private static final String SIZE_ATTR = "_size";
	
    protected SizeMapping(Builder builder) {
        super(builder);

        this.indexName = builder.index;
        this.typeName = builder.type;
        this.payload = builder.source;
        setURI(buildURI());
    }

    @Override
    protected String buildURI() {
        return super.buildURI().replaceFirst(this.typeName, MAPPING_ATTR + "/" + this.typeName);
    }

    @Override
    public String getRestMethodName() {
        return "PUT";
    }

    public static class Builder extends GenericResultAbstractAction.Builder<SizeMapping, Builder> {
        private String index;
        private String type;
        private Object source;

        public Builder(String index, String type, boolean enabled) {
            this.index = index;
            this.type = type;
            
            JSONObject enabledTrue = new JSONObject();
            enabledTrue.put(ENABLED_PROPERTY, enabled);
            
            JSONObject sizeMapping = new JSONObject();
            sizeMapping.put(SIZE_ATTR, enabledTrue);
            
            this.source = sizeMapping;
        }

        @Override
        public SizeMapping build() {
            return new SizeMapping(this);
        }
    }

}
