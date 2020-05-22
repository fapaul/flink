package org.apache.flink.container.entrypoint.jobgraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.runtime.entrypoint.JobClusterEntrypoint} which starts a cluster from a given job graph
 * file.
 */
public class StandaloneJobGraphClusterEntryPoint extends JobClusterEntrypoint {

	@Nonnull
	private final String jobGraphFile;

	private StandaloneJobGraphClusterEntryPoint(
		final Configuration configuration,
		final String jobGraphFile) {
		super(configuration);
		this.jobGraphFile = checkNotNull(jobGraphFile);
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneJobGraphClusterEntryPoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final CommandLineParser<StandaloneJobGraphClusterConfiguration> commandLineParser = new CommandLineParser<>(new StandaloneJobGraphClusterConfigurationParserFactory());

		StandaloneJobGraphClusterConfiguration clusterConfiguration = null;
		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (Exception e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneJobGraphClusterEntryPoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfigurationFromClusterConfig(clusterConfiguration);
		setDefaultExecutionModeIfNotConfigured(configuration);

		StandaloneJobGraphClusterEntryPoint entryPoint = new StandaloneJobGraphClusterEntryPoint(
			configuration,
			clusterConfiguration.getJobGraphFile()
		);

		ClusterEntrypoint.runClusterEntrypoint(entryPoint);
	}

	@VisibleForTesting
	static Configuration loadConfigurationFromClusterConfig(StandaloneJobGraphClusterConfiguration clusterConfiguration) {
		Configuration configuration = loadConfiguration(clusterConfiguration);
		SavepointRestoreSettings.toConfiguration(clusterConfiguration.getSavepointRestoreSettings(), configuration);
		return configuration;
	}

	@VisibleForTesting
	static void setDefaultExecutionModeIfNotConfigured(Configuration configuration) {
		if (isNoExecutionModeConfigured(configuration)) {
			// In contrast to other places, the default for standalone job clusters is ExecutionMode.DETACHED
			configuration.setString(ClusterEntrypoint.EXECUTION_MODE, ExecutionMode.DETACHED.toString());
		}
	}

	private static boolean isNoExecutionModeConfigured(Configuration configuration) {
		return configuration.getString(ClusterEntrypoint.EXECUTION_MODE, null) == null;
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
		return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
			StandaloneResourceManagerFactory.getInstance(),
			new FileJobGraphRetriever(jobGraphFile, tryFindUserLibDirectory().orElse(null))
		);
	}
}
