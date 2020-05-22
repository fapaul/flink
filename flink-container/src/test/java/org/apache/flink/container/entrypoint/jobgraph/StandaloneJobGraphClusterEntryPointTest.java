package org.apache.flink.container.entrypoint.jobgraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link StandaloneJobGraphClusterEntryPoint}.
 */
public class StandaloneJobGraphClusterEntryPointTest extends TestLogger {
	/**
	 * Tests that the default {@link ClusterEntrypoint.ExecutionMode} is {@link ClusterEntrypoint.ExecutionMode#DETACHED}.
	 */
	@Test
	public void testDefaultExecutionModeIsDetached() {
		Configuration configuration = new Configuration();

		StandaloneJobGraphClusterEntryPoint.setDefaultExecutionModeIfNotConfigured(configuration);

		assertThat(getExecutionMode(configuration), equalTo(ClusterEntrypoint.ExecutionMode.DETACHED));
	}

	/**
	 * Tests that {@link ClusterEntrypoint.ExecutionMode} is not overwritten if provided.
	 */
	@Test
	public void testDontOverwriteExecutionMode() {
		Configuration configuration = new Configuration();
		setExecutionMode(configuration, ClusterEntrypoint.ExecutionMode.NORMAL);

		StandaloneJobGraphClusterEntryPoint.setDefaultExecutionModeIfNotConfigured(configuration);

		// Don't overwrite provided configuration
		assertThat(getExecutionMode(configuration), equalTo(ClusterEntrypoint.ExecutionMode.NORMAL));
	}

	private static void setExecutionMode(Configuration configuration, ClusterEntrypoint.ExecutionMode executionMode) {
		configuration.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());
	}

	private static ClusterEntrypoint.ExecutionMode getExecutionMode(Configuration configuration) {
		return ClusterEntrypoint.ExecutionMode.valueOf(configuration.getString(ClusterEntrypoint.EXECUTION_MODE));
	}
}
