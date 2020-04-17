/*
 *  ========================================================================
 *  Helper classes to support simulations of large scale distributed systems
 *  ========================================================================
 *  
 *  This file is part of DistSysJavaHelpers.
 *  
 *    DistSysJavaHelpers is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *   DistSysJavaHelpers is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  (C) Copyright 2016, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 *  (C) Copyright 2012-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 */

package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An implementation of the generic trace file reader functionality to support
 * files from the grid workloads archive (http://gwa.ewi.tudelft.nl/).
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2016"
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems,
 *         MTA SZTAKI (c) 2012-2015"
 */
public class PreziReader extends TraceFileReaderFoundation {

	/**
	 * Constructs a "log" file reader that later on can act as a trace producer for
	 * user side schedulers.
	 * 
	 * @param fileName            The full path to the log file that should act as
	 *                            the source of the jobs produced by this trace
	 *                            producer.
	 * @param from                The first job in the log file that should be
	 *                            produced in the job listing output.
	 * @param to                  The last job in the log file that should be still
	 *                            in the job listing output.
	 * @param allowReadingFurther If true the previously listed "to" parameter is
	 *                            ignored if the "getJobs" function is called on
	 *                            this trace producer.
	 * @param jobType             The class of the job implementation that needs to
	 *                            be produced by this particular trace producer.
	 * @throws SecurityException     If the class of the jobType cannot be accessed
	 *                               by the classloader of the caller.
	 * @throws NoSuchMethodException If the class of the jobType does not hold one
	 *                               of the expected constructors.
	 */
	public PreziReader(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Grid workload format", fileName, from, to, allowReadingFurther, jobType);
	}

	/**
	 * Determines if a particular line in the log file is representing a job
	 * 
	 * Actually ignores empty lines and lines starting with '#'
	 */

	// split line into four pieces (by W/s)
	// check if piece 1 is a number
	// check if piece 2 is a float
	// check if piece 3 if a string (no w/s)
	// check if piece 4 is a string containing url OR default OR export
	@Override
	public boolean isTraceLine(final String line) {
		boolean validLine = false;

		String[] data = line.split(" ");
		try {
			int numberCheck = Integer.parseInt(data[0].toString());
			validLine = true;
		} catch (NumberFormatException e) {
			validLine = false;
		}
		try {
			float floatCheck = Float.parseFloat(data[1]);
			validLine = true;
		} catch (NumberFormatException e) {
			validLine = false;
		}
		if (!data[2].trim().isEmpty()) {
			validLine = true;
		}
		if (data[3].contains("export")) {
			validLine = true;
		}
		if (data[3].contains("url")) {
			validLine = true;
		}
		if (data[3].contains("default")) {
			validLine = true;
		}

		return basicTraceLineDetector("#", line) && validLine;
	}

	/**
	 * Collects the total number of processors in the trace if specified in the
	 * comments
	 */
	@Override
	protected void metaDataCollector(String line) {
		if (line.contains("Processors")) {
			String[] splitLine = line.split("\\s");
			try {
				maxProcCount = parseLongNumber((splitLine[splitLine.length - 1]));
			} catch (NumberFormatException e) {
				// safe to ignore as there is no useful data here then
			}
		}
	}

	/**
	 * Parses a single line of the tracefile and instantiates a job object out of
	 * it.
	 * 
	 * Allows the creation of a job object using the GWA trace line format.
	 * 
	 * Supports GWA traces with millisecond time base (useful to load traces
	 * produced by the ASKALON workflow environment of University of Innsbruck).
	 *
	 * Not the entire LOG trace format is supported.
	 */
	@Override
	public Job createJobFromLine(String jobstring)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		boolean askalon = jobstring.endsWith("ASKALON");
		String[] data = jobstring.trim().split("\\s+");

		if (data[2].contains("error:unsupported-request-method")) {
			return null;
		}
		long jobState = Long.parseLong(data[0]);
		int allocatedProcessors = 1; // Hard coded value for test purposes
		long runTime = 400;
		long waitTime = 0;
		String name = data[2];

		String submitTime = data[1];
		submitTime = submitTime.substring(0, submitTime.indexOf("."));
		long submittedTimeToLong = Long.parseLong(submitTime);

		if (jobState != 1 && (allocatedProcessors < 1 || runTime < 0)) {
			return null;
		} else {
			return jobCreator.newInstance(
					// id
					data[0],
					// submitted time:
					submittedTimeToLong,
					// waiting time:
					Math.max(0, waitTime),
					// run time:
					Math.max(0, runTime),
					// Number of allocated processors
					Math.max(1, allocatedProcessors),
					// average execution time
					300,
					// average memory for job
					512,
					// User name:
					parseTextualField(name),
					// Group membership:
					parseTextualField(name),
					// executable name:
					parseTextualField(name),
					// No preceding job
					null, 0);
		}
	}

	/**
	 * Checks if the particular log line entry contains useful data.
	 * 
	 * @param unparsed the text to be checked for usefulness.
	 * @return the text altered after usefulness checking. If the text is not useful
	 *         then the string "N/A" is returned.
	 */
	private String parseTextualField(final String unparsed) {
		return unparsed.equals("-1") ? "N/A" : unparsed;
		// unparsed.matches("^-?[0-9](?:\\.[0-9])?$")?"N/A":unparsed;
	}

}