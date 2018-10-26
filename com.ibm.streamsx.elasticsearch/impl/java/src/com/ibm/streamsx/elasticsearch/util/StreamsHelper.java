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
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
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
				checker.setInvalidContext("Input port not configured: " + inputPort, new Object[0]);
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
				checker.setInvalidContext("Input attribute not found: " + attrName, new Object[0]);
				return;
			}

			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext("Wrong attribute type for attribute '" + attrName + "', expected: " + attributeType.toString(), new Object[0]);
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
			//checker.setInvalidContext("Input port not configured: " + inputPort, new Object[0]);
			checker.setInvalidContext(Messages.getString("IPORT_NOT_CONFIGURED") + " " + inputPort, new Object[0]);
			return;
		}
		
		// Streams handles all type and name checking on its own if the paramter is specified
		if(!ctx.getParameterNames().contains(attributeParameter)) {
			StreamSchema schema = ctx.getStreamingInputs().get(inputPort).getStreamSchema();
			Attribute attr = schema.getAttribute(defaultAttribute);	
			
			// check existence of attribute 
			if (attr == null) {
				//checker.setInvalidContext("Default attribute not found: " + defaultAttribute, new Object[0]);
				checker.setInvalidContext(Messages.getString("DEFATTR_NOT_FOUND") + " " + defaultAttribute, new Object[0]);
				return;
			}
			
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				//checker.setInvalidContext("Wrong attribute type for attribute '" + defaultAttribute + "', expected: " + attributeType.toString(), new Object[0]);
				checker.setInvalidContext(Messages.getString("WRONG_ATTR_TYPE"), new Object[] { defaultAttribute, attributeType.toString() } );
				return;
			}
		}
	}
	
	public static void validateInputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateInputAttribute(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}

	/**
	 * check that a suitable attribute is availabe in the operator output stream
	 * @param checker 
	 * @param attributeParameter
	 * @param defaultAttribute
	 * @param attributeType
	 * @param outputPort
	 * @param attrSchema the expected tuple schema of the attribute, null if not needed
	 */
	public static void validateOutputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int outputPort, StreamSchema expSchema) {
		OperatorContext ctx = checker.getOperatorContext();
		
		// cannot validate parameter values at compile time, only check for default attribute here
		if(!ctx.getParameterNames().contains(attributeParameter) && defaultAttribute != null) {

			// check that we have the port
			List<StreamingOutput<OutputTuple>> outputPorts = ctx.getStreamingOutputs();
			if (outputPort >= outputPorts.size()) {
				//checker.setInvalidContext("Output port not configured: " + outputPort, new Object[0]);
				checker.setInvalidContext(Messages.getString("OPORT_NOT_CONFIGURED") + " " + outputPort, new Object[0]);
				return;
			}
			
			StreamSchema schema = ctx.getStreamingOutputs().get(outputPort).getStreamSchema();
			Attribute attr = schema.getAttribute(defaultAttribute);
			
			// check existence of attribute 
			if (attr == null) {
				//checker.setInvalidContext("Default attribute not found: " + defaultAttribute, new Object[0]);
				checker.setInvalidContext(Messages.getString("DEFATTR_NOT_FOUND") + " " + defaultAttribute, new Object[0]);
				return;
			}
			
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				//checker.setInvalidContext("Wrong attribute type for attribute '" + defaultAttribute + "', expected: " + attributeType.toString(), new Object[0]);
				checker.setInvalidContext(Messages.getString("WRONG_ATTR_TYPE"), new Object[] { defaultAttribute, attributeType.toString() } );
				return;
			}
			
			// check tuple schema if specified
			if (null != expSchema) {
				// attribute schema 
				Tuple t1 = schema.getTuple().getTuple(defaultAttribute);
				StreamSchema attrSchema = t1.getStreamSchema();
				String attrSpec = attrSchema.toString();
				//System.err.println("ATR SCHEMA : " + attrSpec);
				
				// expected schema
				String expSpec = expSchema.toString();
				//System.err.println("EXP SCHEMA : " + expSpec);
				
				// compare schemas
				// TODO change to more robust compare logic
				if (!attrSpec.equals(expSpec)) {
					//System.err.println("NOT EQUAL");
					checker.setInvalidContext(Messages.getString("ATTR_TYPE_ERROR") + " " + defaultAttribute + " : " + attrSpec, new Object[0] );
				}
			}
		}
	}

	public static void validateOutputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int outputPort) {
		validateOutputAttribute(checker, attributeParameter, defaultAttribute, attributeType, outputPort, null);
	}

	public static void validateOutputAttribute(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateOutputAttribute(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}
	
	/**
	 * check that a parameter specified output attribute is availabe in the operator output stream at runtime, with correct type 
	 * @param checker
	 * @param attributeParameter
	 * @param defaultAttribute
	 * @param attributeType
	 * @param outputPort
	 */
	public static void validateOutputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType, int outputPort, StreamSchema expSchema) {
		OperatorContext ctx = checker.getOperatorContext();

		if(ctx.getParameterNames().contains(attributeParameter)) {
			
			// check that we have an output port at all
			List<StreamingOutput<OutputTuple>> outputPorts = ctx.getStreamingOutputs();
			if (outputPort >= outputPorts.size()) {
				checker.setInvalidContext("Output port not configured: " + outputPort, new Object[0]);
				return;
			}
			
			Attribute attr = null;
            String attrName = ctx.getParameterValues(attributeParameter).get(0);
            StreamSchema schema = ctx.getStreamingOutputs().get(outputPort).getStreamSchema();
            attr = schema.getAttribute(attrName);

            // check existence of attribute
            if(attr == null) {
            	checker.setInvalidContext("Output attribute not found: " + attrName ,new Object[0]);
            	return;
		    }
            
			// check correct type of attribute
			if(!checker.checkAttributeType(attr, attributeType)) {
				checker.setInvalidContext("Wrong attribute type for attribute '" + attrName + "', expected: " + attributeType.toString(), new Object[0]);
				return;
			}
			
			// check tuple schema if specified
			if (null != expSchema) {
				// attribute schema 
				Tuple t1 = schema.getTuple().getTuple(attrName);
				StreamSchema attrSchema = t1.getStreamSchema();
				String attrSpec = attrSchema.toString();
				//System.err.println("ATR SCHEMA : " + attrSpec);
				
				// expected schema
				String expSpec = expSchema.toString();
				//System.err.println("EXP SCHEMA : " + expSpec);
				
				// compare schemas
				// TODO change to more robust compare logic
				if (!attrSpec.equals(expSpec)) {
					//System.err.println("NOT EQUAL");
					checker.setInvalidContext("Attribute type error : " + attrName + " : " + attrSpec, new Object[0] );
				}
			}
			
		}
	}

	public static void validateOutputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType,  int outputPort) {
		validateOutputAttributeRuntime(checker, attributeParameter, defaultAttribute, attributeType, outputPort, null);
	}

	public static void validateOutputAttributeRuntime(OperatorContextChecker checker, String attributeParameter, String defaultAttribute, MetaType attributeType) {
		validateOutputAttributeRuntime(checker, attributeParameter, defaultAttribute, attributeType, 0);
	}

	// range check for double parameters
	public static void validateDoubleParamRange(OperatorContextChecker checker, String parameterName, double lowBound, double upBound) {
		OperatorContext ctx = checker.getOperatorContext();
		
		if(ctx.getParameterNames().contains(parameterName)) {
            String paramValue = ctx.getParameterValues(parameterName).get(0);
            
            // it is ok to not check parse error here, because the param type is ensured by Streams
            double val = Double.parseDouble(paramValue);
            
            if (val < lowBound ) {
				checker.setInvalidContext("Parameter '" + parameterName + "' must not be less than '" + lowBound + "' .", new Object[0]);            	
            }
            if (val > upBound ) {
				checker.setInvalidContext("Parameter '" + parameterName + "' must not be higher than '" + upBound + "' .", new Object[0]);
            }
		}
	}

	// range check for int parameters
	public static void validateIntParamRange(OperatorContextChecker checker, String parameterName, int lowBound, int upBound) {
		OperatorContext ctx = checker.getOperatorContext();
		
		if(ctx.getParameterNames().contains(parameterName)) {
            String paramValue = ctx.getParameterValues(parameterName).get(0);
            
            // it is ok to not check parse error here, because the param type is ensured by Streams
            int val = Integer.parseInt(paramValue);
            
            if (val < lowBound ) {
				checker.setInvalidContext("Parameter '" + parameterName + "' must not be less than '" + lowBound + "' .", new Object[0]);            	
            }
            if (val > upBound ) {
				checker.setInvalidContext("Parameter '" + parameterName + "' must not be higher than '" + upBound + "' .", new Object[0]);
            }
		}
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
	
}
