package org.embulk.input.aws_cost_explorer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.embulk.config.ConfigException;
import software.amazon.awssdk.services.costexplorer.model.Dimension;
import software.amazon.awssdk.services.costexplorer.model.GroupDefinitionType;

public class GroupsConfigValidator
{
    private GroupsConfigValidator()
    {
    }

    /**
     * Validates the groups configuration.
     *
     * @param groups the groups configuration
     */
    public static void validate(List<Map<String, String>> groups)
    {
        if (groups.isEmpty()) {
            return;
        }

        if (groups.size() > 2) {
            throw new ConfigException("groups must have at most 2 elements.");
        }

        for (Map<String, String> group : groups) {
            validateGroup(group);
        }

        validateDuplicateGroups(groups);
    }

    private static void validateGroup(Map<String, String> group)
    {
        final String groupType = group.get("type");
        final String groupKey = group.get("key");

        if (groupType == null || groupKey == null) {
            throw new ConfigException("group must have a type and a key.");
        }

        if (GroupDefinitionType.DIMENSION.name().equals(groupType)) {
            validateDimensionGroupKey(groupKey);
            return;
        }

        for (GroupDefinitionType enumEntry : GroupDefinitionType.values()) {
            if (enumEntry.toString().equals(groupType)) {
                validateGroupKey(groupKey);
                return;
            }
        }

        throw new ConfigException("group type must be one of the following: "
                + Arrays.stream(GroupDefinitionType.values()).map(Enum::name).collect(Collectors.joining(", ")));
    }

    private static void validateDimensionGroupKey(String groupKey)
    {
        for (Dimension enumEntry : Dimension.values()) {
            if (enumEntry.name().equals(groupKey)) {
                return;
            }
        }

        throw new ConfigException("dimension group key must be one of the following: "
                + Arrays.stream(Dimension.values()).map(Enum::name).collect(Collectors.joining(", ")));
    }

    private static void validateGroupKey(String groupKey)
    {
        if (!groupKey.isEmpty() && groupKey.length() <= 1024) {
            return;
        }

        throw new ConfigException("group key must be non-empty and at most 1024 characters long.");
    }

    private static void validateDuplicateGroups(List<Map<String, String>> groups)
    {
        Set<String> typeAndKeyPairs = new HashSet<>();

        for (Map<String, String> group : groups) {
            String type = group.get("type");
            String key = group.get("key");
            String pair = type + ":" + key;

            if (!typeAndKeyPairs.add(pair)) {
                throw new ConfigException("groups must not have duplicate type and key pairs.");
            }
        }
    }
}
