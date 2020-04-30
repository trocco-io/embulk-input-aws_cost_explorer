Embulk::JavaPlugin.register_input(
  "aws_cost_explorer", "org.embulk.input.aws_cost_explorer.AwsCostExplorerInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
