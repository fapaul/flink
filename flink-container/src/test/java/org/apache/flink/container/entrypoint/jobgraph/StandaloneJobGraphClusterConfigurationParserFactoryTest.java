package org.apache.flink.container.entrypoint.jobgraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests for the {@link StandaloneJobGraphClusterConfigurationParserFactory}.
 */
public class StandaloneJobGraphClusterConfigurationParserFactoryTest extends TestLogger {

	private static final CommandLineParser<StandaloneJobGraphClusterConfiguration> commandLineParser = new CommandLineParser<>(new StandaloneJobGraphClusterConfigurationParserFactory());

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	private String confDirPath;

	@Before
	public void createEmptyFlinkConfiguration() throws IOException {
		File confDir = tempFolder.getRoot();
		confDirPath = confDir.getAbsolutePath();
		File confFile = new File(confDir, GlobalConfiguration.FLINK_CONF_FILENAME);
		confFile.createNewFile();
	}

	@Test
	public void testParseJobGraphFile() throws FlinkParseException {
		String jobGraphFile = "/test/foo";
		final String[] args = {"--configDir", confDirPath, "--job-graph", jobGraphFile};

		final StandaloneJobGraphClusterConfiguration clusterConfiguration = commandLineParser.parse(args);
		assertThat(clusterConfiguration.getJobGraphFile(), is(equalTo(jobGraphFile)));
	}

	@Test
	public void testEntrypointClusterConfigurationToConfigurationParsing() throws FlinkParseException {
		String jobGraphFile = "/test/foo";
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("/test/savepoint/path", true);
		final String key = DeploymentOptions.TARGET.key();
		final String value = "testDynamicExecutorConfig";
		final int restPort = 1234;
		final String arg1 = "arg1";
		final String arg2 = "arg2";
		final String[] args = {
			"--configDir", confDirPath,
			"--fromSavepoint", savepointRestoreSettings.getRestorePath(),
			"--allowNonRestoredState",
			"--webui-port", String.valueOf(restPort),
			"--job-graph", jobGraphFile,
			String.format("-D%s=%s", key, value),
			arg1, arg2};

		final StandaloneJobGraphClusterConfiguration clusterConfiguration = commandLineParser.parse(args);
		Assert.assertThat(clusterConfiguration.getArgs(), arrayContaining(arg1, arg2));

		final Configuration configuration = StandaloneJobGraphClusterEntryPoint
			.loadConfigurationFromClusterConfig(clusterConfiguration);

		Assert.assertThat(SavepointRestoreSettings.fromConfiguration(configuration), Matchers.is(Matchers.equalTo(savepointRestoreSettings)));

		Assert.assertThat(configuration.get(RestOptions.PORT), Matchers.is(Matchers.equalTo(restPort)));
		Assert.assertThat(configuration.get(DeploymentOptions.TARGET), Matchers.is(Matchers.equalTo(value)));
	}
}
