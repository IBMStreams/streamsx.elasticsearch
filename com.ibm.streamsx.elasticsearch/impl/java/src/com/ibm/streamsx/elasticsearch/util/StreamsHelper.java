//
// ****************************************************************************
// * Copyright (C) 2018, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch.util;

import java.util.List;

import com.ibm.streamsx.elasticsearch.i18n.Messages;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.state.CheckpointContext;
import com.ibm.streams.operator.state.ConsistentRegionContext;

/**
 * contains methods to validate compile and runtime checking
 *
 */
public class StreamsHelper {

	
	public static void validateInputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int inputPort) {
		OperatorContext ctx = checker.getOperatorContext();

		if(ctx.getParameterNames().contains(attributeParameter)) {
			
			// check port availability
			List<StreamingInput<Tuple>> inputPorts = ctx.getStreamingInputs();
			if (inputPort >= inputPorts.size()) {
				checker.setInvalidContext(Messages.getString("ELASTICSEARCH_IPORT_NOT_CONFIGURED") + " " + inputPort, new Object[0]);
				return;
			}
			
			// TODO : parse attribute name from a string like : iport$0.get_observationx()
			// where observationx is the actual name 
			String attrSpec = ctx.getParameterValues(attributeParameter).get(0);
			String attrName = "";
			
			// derive attribute from attribute name
			StreamSchema schema = ctx.getStreamingInputs().get(inputPort).getStreamSchema();
			Attribute attr = null;
			attr = schema.getAttribute(attrName);

			// check existence of attribute 
			if (attr == null) {
				checker.setInvalidContext(Messages.getString("ELASTICSEARCH_ATTR_NOT_FOUND") + " " + defaultAttribute, new Object[0]);
				return;
			}

			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext(Messages.getString("ELASTICSEARCH_WRONG_ATTR_TYPE"), new Object[] { defaultAttribute, attributeType.toString() } );
				return;
			}
		
		}
	}
	
	public static void validateInputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateInputAttributeRuntime(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}
	
	/**
	 * check that a suitable attribute is availabe in the operator input stream
	 * @param checker the context checker 
	 * @param attributeParameter the parameter name specifying the attribute name
	 * @param defaultAttribute the default attribute name, used if the parameter is not set
	 * @param attributeType the type of the attribute
	 * @param inputPort the number of the port
	 */
	public static void validateInputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int inputPort) {
		OperatorContext ctx = checker.getOperatorContext();

		List<StreamingInput<Tuple>> inputPorts = ctx.getStreamingInputs();
		if (inputPort >= inputPorts.size()) {
			checker.setInvalidContext(Messages.getString("ELASTICSEARCH_IPORT_NOT_CONFIGURED") + " " + inputPort, new Object[0]);
			return;
		}
		
		// Streams handles all type and name checking on its own if the paramter is specified
		if(!ctx.getParameterNames().contains(attributeParameter)) {
			StreamSchema schema = ctx.getStreamingInputs().get(inputPort).getStreamSchema();
			Attribute attr = schema.getAttribute(defaultAttribute);	
			
			// check existence of attribute 
			if (attr == null) {
				checker.setInvalidContext(Messages.getString("ELASTICSEARCH_ATTR_NOT_FOUND") + " " + defaultAttribute, new Object[0]);
				return;
			}
			
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext(Messages.getString("ELASTICSEARCH_WRONG_ATTR_TYPE"), new Object[] { defaultAttribute, attributeType.toString() } );
				return;
			}
		}
	}
	
	public static void validateInputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateInputAttribute(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}

	public static void checkCheckpointConfig(OperatorContextChecker checker, String operatorName) {
		OperatorContext opContext = checker.getOperatorContext();		
		CheckpointContext chkptContext = opContext.getOptionalContext(CheckpointContext.class);
		if (chkptContext != null) {
			if (chkptContext.getKind().equals(CheckpointContext.Kind.OPERATOR_DRIVEN)) {
				checker.setInvalidContext(
						Messages.getString("ELASTICSEARCH_NOT_CHECKPOINT_OPERATOR_DRIVEN", operatorName), null);
			}
			if (chkptContext.getKind().equals(CheckpointContext.Kind.PERIODIC)) {
				checker.setInvalidContext(
						Messages.getString("ELASTICSEARCH_NOT_CHECKPOINT_PERIODIC", operatorName), null);
			}			
		}
	}
	
	public static void checkConsistentRegion(OperatorContextChecker checker, String operatorName) {
		// check that the sink operator is not at the start of the consistent region
		OperatorContext opContext = checker.getOperatorContext();
		ConsistentRegionContext crContext = opContext.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null) {
			if (crContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("ELASTICSEARCH_NOT_CONSISTENT_REGION_START", operatorName), null); 
			}
		}		
	}

	public static boolean isPositiveInteger(String str) {
		int d;
	    try {
			d = Integer.parseInt(str);
	    } catch (NumberFormatException | NullPointerException nfe) {
	        return false;
	    }
	    if (d < 0) {
	    	return false;
	    }
	    return true;
	}	
}
