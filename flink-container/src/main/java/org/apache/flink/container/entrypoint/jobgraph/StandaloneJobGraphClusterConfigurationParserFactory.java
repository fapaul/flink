package org.apache.flink.container.entrypoint.jobgraph;

import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

/**
 * Parser factory which generates a {@link StandaloneJobGraphClusterConfiguration} from a given
 * list of command line arguments.
 */
public class StandaloneJobGraphClusterConfigurationParserFactory implements ParserResultFactory<StandaloneJobGraphClusterConfiguration> {

	private static final Option JOB_GRAPH_FILE = Option.builder("jgraph")
		.longOpt("job-graph")
		.required(true)
		.hasArg(true)
		.argName("job graph file")
		.desc("File path to the job graph.")
		.build();

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(CONFIG_DIR_OPTION);
		options.addOption(DYNAMIC_PROPERTY_OPTION);
		options.addOption(REST_PORT_OPTION);
		options.addOption(JOB_GRAPH_FILE);
		options.addOption(CliFrontendParser.SAVEPOINT_PATH_OPTION);
		options.addOption(CliFrontendParser.SAVEPOINT_ALLOW_NON_RESTORED_OPTION);

		return options;
	}

	@Override
	public StandaloneJobGraphClusterConfiguration createResult(@Nonnull CommandLine commandLine) throws FlinkParseException {
		final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		final int restPort = getRestPort(commandLine);
		final String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());
		final SavepointRestoreSettings savepointRestoreSettings = CliFrontendParser.createSavepointRestoreSettings(commandLine);
		final String jobGraphFile = commandLine.getOptionValue(JOB_GRAPH_FILE.getOpt());

		return new StandaloneJobGraphClusterConfiguration(
			configDir,
			dynamicProperties,
			commandLine.getArgs(),
			hostname,
			restPort,
			savepointRestoreSettings,
			jobGraphFile
		);
	}

	private int getRestPort(CommandLine commandLine) throws FlinkParseException {
		final String restPortString = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
		try {
			return Integer.parseInt(restPortString);
		} catch (NumberFormatException e) {
			throw createFlinkParseException(REST_PORT_OPTION, e);
		}
	}

	private static FlinkParseException createFlinkParseException(Option option, Exception cause) {
		return new FlinkParseException(String.format("Failed to parse '--%s' option", option.getLongOpt()), cause);
	}
}
