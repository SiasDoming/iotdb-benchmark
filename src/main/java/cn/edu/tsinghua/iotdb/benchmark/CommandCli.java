package cn.edu.tsinghua.iotdb.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

public class CommandCli {
	private final String HELP_ARGS = "h";
	private final String HELP_NAME = "help";
	
	private final String CONFIG_ARGS = "cf";
	private final String CONFIG_NAME = "config file";

	private final String LIST_ARGS = "lf";
	private final String LIST_NAME = "UDF list file";
	
	private static final int MAX_HELP_CONSOLE_WIDTH = 88;
	
	private Options createOptions() {
		Options options = new Options();
		Option help = Option.builder(HELP_ARGS)
				.argName(HELP_NAME)
				.hasArg(false)
				.desc("Display help information")
				.required(false)
				.build();
		options.addOption(help);
		
		Option config = Option.builder(CONFIG_ARGS)
				.argName(CONFIG_NAME)
				.hasArg()
				.desc("Config file path (default set as ./conf/config.properties)")
				.required(false)
				.build();
		options.addOption(config);

		Option list = Option.builder(LIST_ARGS)
				.argName(LIST_NAME)
				.hasArg()
				.desc("UDF list json file path (default set as ./conf/TestUDFList.json)")
				.required(false)
				.build();
		options.addOption(list);

		return options;
	}
	
	private boolean parseParams(CommandLineParser parser, Options options, String[] args, HelpFormatter hf){
		try {
			CommandLine commandLine = parser.parse(options, args);
			if (commandLine.hasOption(HELP_ARGS)) {
				hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
				return false;
			}

			if(commandLine.hasOption(CONFIG_ARGS)) {
				System.setProperty(Constants.BENCHMARK_CONF, commandLine.getOptionValue(CONFIG_ARGS));
			}

			if(commandLine.hasOption(LIST_ARGS)) {
				System.setProperty(Constants.BENCHMARK_UDF_LIST, commandLine.getOptionValue(LIST_ARGS));
			}
			
		} catch (ParseException e) {
			System.out.println("Require more params input, please check the following hint.");
			hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
			return false;
		} catch (Exception e) {
			System.out.println("Error params input, because "+e.getMessage());
			hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
			return false;
		}
		return true;
	}
	
	public boolean init(String[] args){
		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLineParser parser = new DefaultParser();

		if (args == null || args.length == 0) {
			return true;
		} else {
			return parseParams(parser, options, args, hf);
		}
	}
	
	public static void main(String[] args) {

	}

}
