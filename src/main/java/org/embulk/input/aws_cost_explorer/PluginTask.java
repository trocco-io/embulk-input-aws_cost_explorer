package org.embulk.input.aws_cost_explorer;

import java.util.List;
import java.util.Map;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface PluginTask extends Task {
    @Config("access_key_id")
    String getAccessKeyId();

    @Config("secret_access_key")
    String getSecretAccessKey();

    @Config("metrics")
    @ConfigDefault("\"UnblendedCost\"")
    String getMetrics();

    @Config("groups")
    @ConfigDefault("[]")
    List<Map<String, String>> getGroups();

    @Config("start_date")
    String getStartDate();

    @Config("end_date")
    String getEndDate();
}
