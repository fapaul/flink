package org.apache.flink.container.entrypoint.jobgraph;

import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for the {@link StandaloneJobGraphClusterEntryPoint}.
 */
public class StandaloneJobGraphClusterConfiguration extends EntrypointClusterConfiguration {
	@Nonnull
	private final SavepointRestoreSettings savepointRestoreSettings;
	@Nonnull
	private final String jobGraphFile;

	public StandaloneJobGraphClusterConfiguration(
		@Nonnull String configDir,
		@Nonnull Properties dynamicProperties,
		@Nonnull String[] args,
		@Nullable String hostname,
		int restPort,
		@Nonnull SavepointRestoreSettings savepointRestoreSettings,
		@Nonnull String jobGraphFile
	) {
		super(configDir, dynamicProperties, args, hostname, restPort);
		this.savepointRestoreSettings = checkNotNull(savepointRestoreSettings);
		this.jobGraphFile = checkNotNull(jobGraphFile);
	}

	@Nonnull
	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointRestoreSettings;
	}

	@Nonnull
	public String getJobGraphFile() {
		return jobGraphFile;
	}
}
