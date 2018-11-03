//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.elasticsearch.sample.ECG;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * A source operator that outputs ECG (Lead II) data. The data is read from the 
 * ecgII.csv file located in the project's data directory. Each line corresponds 
 * to an ECG value, which are outputted in intervals of 8 milliseconds. When the 
 * file is finished being read, a final punctuation marker is outputted.
 */
@PrimitiveOperator(
		name="ECGSimulator",
		namespace="com.ibm.streamsx.elasticsearch.sample.ECG",
		description="Java Operator ECGSimulator"
		)
@OutputPorts({
		@OutputPortSet(
				description="Port that produces tuples",
				cardinality=1,
				optional=false,
				windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating
				)
		})
public class ECGSimulator extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Static variables.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(ECGSimulator.class.getName());
	
    /**
     * ECG value attribute name.
     */
    private static final String ECG_VALUE = "ecg_value";
    
    /**
     * Time-stamp attribute name.
     */
    private static final String TIMESTAMP = "ecg_timestamp";
    
    /**
     * Separator value.
     */
    private static final String SEPARATOR = ",";
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	@Parameter(
			optional=false,
			description="Path to CSV file to read."
			)
	public void setCsvPath(String csvPath) {
		this.csvPath = csvPath;
	}
    
	/**
	 * Thread for calling <code>produceTuples()</code> to produce tuples 
	 */
    private Thread processThread;
    
    /**
     * Path to CSV file to read inside data directory.
     */
    private String csvPath;
    
    /**
     * Reader to read CSV file.
     */
    BufferedReader reader;

    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        /*
         * Create the thread for producing tuples. 
         * The thread is created at initialize time but started.
         * The thread will be started by allPortsReady().
         */
        processThread = getOperatorContext().getThreadFactory().newThread(
                new Runnable() {

                    @Override
                    public void run() {
                        try {
                            produceTuples();
                        } catch (Exception e) {
                            Logger.getLogger(this.getClass()).error("Operator error", e);
                        }                    
                    }
                    
                });
        
        /*
         * Set the thread not to be a daemon to ensure that the SPL runtime
         * will wait for the thread to complete before determining the
         * operator is complete.
         */
        processThread.setDaemon(false);
    }
    
	/**
	 * Notification that initialization is complete and all input and output ports 
	 * are connected and ready to receive and submit tuples.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		OperatorContext context = getOperatorContext();
		_trace.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
		
		// Start a thread for producing tuples because operator 
		// implementations must not block and must return control to the caller.
		processThread.start();
	}
    
    /**
     * Submit new tuples to the output stream
     * @throws Exception if an error occurs while submitting a tuple
     */
    private void produceTuples() throws Exception  {
        final StreamingOutput<OutputTuple> out = getOutput(0);
        
        try {
        	reader = new BufferedReader(new FileReader(csvPath));
            String line = "";
        	while ((line = reader.readLine()) != null) {
        		
        		// Get ECG data.
        		String[] lineValues = line.split(SEPARATOR);
        		double value = Double.valueOf(lineValues[1]);
        		long time = System.currentTimeMillis();
        		
        		// Set value in new tuple.
                OutputTuple tuple = out.newTuple();
                tuple.setDouble(ECG_VALUE, value);
                tuple.setLong(TIMESTAMP, time);
        		
        		// Output new tuple to output stream.
                out.submit(tuple);
                
                // Interval between each ECG value.
                Thread.sleep(8);
        	}
        } catch (IOException e) {
        	_trace.error(e);
        } finally {
        	if (reader != null) {
        		try {
        			reader.close();
        		} catch (IOException e) {
        			_trace.error(e);
        		}
        	}
        }
        
        // Mark end of outputting tuple data.
        out.punctuate(Punctuation.FINAL_MARKER);
    }

    /**
     * Shutdown this operator, which will interrupt the thread
     * executing the <code>produceTuples()</code> method.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        if (processThread != null) {
            processThread.interrupt();
            processThread = null;
        }
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        super.shutdown();
    }
}
