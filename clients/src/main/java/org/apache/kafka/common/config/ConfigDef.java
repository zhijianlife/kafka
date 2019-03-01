/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.config;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class is used for specifying the set of expected configurations. For each configuration, you can specify
 * the name, the type, the default value, the documentation, the group information, the order in the group,
 * the width of the configuration value and the name suitable for display in the UI.
 *
 * You can provide special validation logic used for single configuration validation by overriding {@link Validator}.
 *
 * Moreover, you can specify the dependents of a configuration. The valid values and visibility of a configuration
 * may change according to the values of other configurations. You can override {@link Recommender} to get valid
 * values and set visibility of a configuration given the current configuration values.
 *
 * <p/>
 * To use the class:
 * <p/>
 * <pre>
 * ConfigDef defs = new ConfigDef();
 *
 * defs.define(&quot;config_with_default&quot;, Type.STRING, &quot;default string value&quot;, &quot;Configuration with default value.&quot;);
 * defs.define(&quot;config_with_validator&quot;, Type.INT, 42, Range.atLeast(0), &quot;Configuration with user provided validator.&quot;);
 * defs.define(&quot;config_with_dependents&quot;, Type.INT, &quot;Configuration with dependents.&quot;, &quot;group&quot;, 1, &quot;Config With Dependents&quot;, Arrays.asList(&quot;config_with_default;&quot;,&quot;config_with_validator&quot;));
 *
 * Map&lt;String, String&gt; props = new HashMap&lt;&gt();
 * props.put(&quot;config_with_default&quot;, &quot;some value&quot;);
 * props.put(&quot;config_with_dependents&quot;, &quot;some other value&quot;);
 * // will return &quot;some value&quot;
 * Map&lt;String, Object&gt; configs = defs.parse(props);
 * String someConfig = (String) configs.get(&quot;config_with_default&quot;);
 * // will return default value of 42
 * int anotherConfig = (Integer) configs.get(&quot;config_with_validator&quot;);
 *
 * To validate the full configuration, use:
 * List&lt;Config&gt; configs = def.validate(props);
 * The {@link Config} contains updated configuration information given the current configuration values.
 * </pre>
 * <p/>
 * This class can be used standalone or in combination with {@link AbstractConfig} which provides some additional
 * functionality for accessing configs.
 */
public class ConfigDef {

    public static final Object NO_DEFAULT_VALUE = "";

    private final Map<String, ConfigKey> configKeys;
    private final List<String> groups;
    private Set<String> configsWithNoParent;

    public ConfigDef() {
        configKeys = new HashMap<>();
        groups = new LinkedList<>();
        configsWithNoParent = null;
    }

    public ConfigDef(ConfigDef base) {
        configKeys = new HashMap<>(base.configKeys);
        groups = new LinkedList<>(base.groups);
        configsWithNoParent = base.configsWithNoParent == null ? null : new HashSet<>(base.configsWithNoParent);
    }

    /**
     * Returns unmodifiable set of properties names defined in this {@linkplain ConfigDef}
     *
     * @return new unmodifiable {@link Set} instance containing the keys
     */
    public Set<String> names() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    public ConfigDef define(ConfigKey key) {
        if (configKeys.containsKey(key.name)) {
            throw new ConfigException("Configuration " + key.name + " is defined twice.");
        }
        if (key.group != null && !groups.contains(key.group)) {
            groups.add(key.group);
        }
        configKeys.put(key.name, key);
        return this;
    }

    /**
     * Define a new configuration
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param validator the validator to use in checking the correctness of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @param recommender the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(new ConfigKey(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender, false));
    }

    /**
     * Define a new configuration with no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param validator the validator to use in checking the correctness of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no dependents
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param validator the validator to use in checking the correctness of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param recommender the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no dependents and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param validator the validator to use in checking the correctness of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no special validation logic
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @param recommender the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    /**
     * Define a new configuration with no special validation logic and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no special validation logic and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param recommender the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no special validation logic, not dependents and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @param recommender the recommender provides valid values given the parent configuration value
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    /**
     * Define a new configuration with no default value, no special validation logic and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param dependents the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no default value, no special validation logic and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @param recommender the recommender provides valid values given the parent configuration value
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no default value, no special validation logic, no dependents and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @param group the group this config belongs to
     * @param orderInGroup the order of this config in the group
     * @param width the width of the config
     * @param displayName the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no group, no order in group, no width, no display name, no dependents and no custom recommender
     *
     * @param name the name of the config parameter
     * @param type the type of the config
     * @param defaultValue the default value to use if this config isn't present
     * @param validator the validator to use in checking the correctness of the config
     * @param importance the importance of this config
     * @param documentation the documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation) {
        return define(name, type, defaultValue, validator, importance, documentation, null, -1, Width.NONE, name);
    }

    /**
     * Define a new configuration with no special validation logic
     *
     * @param name The name of the config parameter
     * @param type The type of the config
     * @param defaultValue The default value to use if this config isn't present
     * @param importance The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation) {
        return define(name, type, defaultValue, null, importance, documentation);
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     *
     * @param name The name of the config parameter
     * @param type The type of the config
     * @param importance The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
    }

    /**
     * Define a new internal configuration. Internal configuration won't show up in the docs and aren't
     * intended for general use.
     *
     * @param name The name of the config parameter
     * @param type The type of the config
     * @param defaultValue The default value to use if this config isn't present
     * @param importance
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef defineInternal(final String name, final Type type, final Object defaultValue, final Importance importance) {
        return define(new ConfigKey(name, type, defaultValue, null, importance, "", "", -1, Width.NONE, name, Collections.<String>emptyList(), null, true));
    }

    /**
     * Get the configuration keys
     *
     * @return a map containing all configuration keys
     */
    public Map<String, ConfigKey> configKeys() {
        return configKeys;
    }

    /**
     * Get the groups for the configuration
     *
     * @return a list of group names
     */
    public List<String> groups() {
        return groups;
    }

    /**
     * Add standard SSL client configuration options.
     *
     * @return this
     */
    public ConfigDef withClientSslSupport() {
        SslConfigs.addClientSslSupport(this);
        return this;
    }

    /**
     * Add standard SASL client configuration options.
     *
     * @return this
     */
    public ConfigDef withClientSaslSupport() {
        SaslConfigs.addClientSaslSupport(this);
        return this;
    }

    /**
     * Parse and validate configs against this configuration definition. The input is a map of configs.
     * It is expected that the keys of the map are strings, but the values can either be strings
     * or they may already be of the appropriate type (int, string, etc).
     * This will work equally well with either java.util.Properties instances or a programmatically constructed map.
     *
     * @param props The configs to parse and validate.
     * @return Parsed and validated configs.
     * The key will be the config name and the value will be the value parsed into the appropriate type (int, string, etc).
     */
    public Map<String, Object> parse(Map<?, ?> props) {
        // Check all configurations are defined
        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        if (!undefinedConfigKeys.isEmpty()) {
            String joined = Utils.join(undefinedConfigKeys, ",");
            throw new ConfigException("Some configurations in are referred in the dependents, but not defined: " + joined);
        }
        // parse all known keys
        Map<String, Object> values = new HashMap<>();
        for (ConfigKey key : configKeys.values()) {
            Object value;
            // props map contains setting - assign ConfigKey value
            if (props.containsKey(key.name)) {
                value = parseType(key.name, props.get(key.name), key.type);
                // props map doesn't contain setting, the key is required because no default value specified - its an error
            } else if (key.defaultValue == NO_DEFAULT_VALUE) {
                throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
            } else {
                // otherwise assign setting its default value
                value = key.defaultValue;
            }
            if (key.validator != null) {
                key.validator.ensureValid(key.name, value);
            }
            values.put(key.name, value);
        }
        return values;
    }

    /**
     * Validate the current configuration values with the configuration definition.
     *
     * @param props the current configuration values
     * @return List of Config, each Config contains the updated configuration information given
     * the current configuration values.
     */
    public List<ConfigValue> validate(Map<String, String> props) {
        return new ArrayList<>(validateAll(props).values());
    }

    public Map<String, ConfigValue> validateAll(Map<String, String> props) {
        Map<String, ConfigValue> configValues = new HashMap<>();
        for (String name : configKeys.keySet()) {
            configValues.put(name, new ConfigValue(name));
        }

        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        for (String undefinedConfigKey : undefinedConfigKeys) {
            ConfigValue undefinedConfigValue = new ConfigValue(undefinedConfigKey);
            undefinedConfigValue.addErrorMessage(undefinedConfigKey + " is referred in the dependents, but not defined.");
            undefinedConfigValue.visible(false);
            configValues.put(undefinedConfigKey, undefinedConfigValue);
        }

        Map<String, Object> parsed = parseForValidate(props, configValues);
        return validate(parsed, configValues);
    }

    // package accessible for testing
    Map<String, Object> parseForValidate(Map<String, String> props, Map<String, ConfigValue> configValues) {
        Map<String, Object> parsed = new HashMap<>();
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name : configsWithNoParent) {
            parseForValidate(name, props, parsed, configValues);
        }
        return parsed;
    }

    private Map<String, ConfigValue> validate(Map<String, Object> parsed, Map<String, ConfigValue> configValues) {
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name : configsWithNoParent) {
            validate(name, parsed, configValues);
        }
        return configValues;
    }

    private List<String> undefinedDependentConfigs() {
        Set<String> undefinedConfigKeys = new HashSet<>();
        for (String configName : configKeys.keySet()) {
            ConfigKey configKey = configKeys.get(configName);
            List<String> dependents = configKey.dependents;
            for (String dependent : dependents) {
                if (!configKeys.containsKey(dependent)) {
                    undefinedConfigKeys.add(dependent);
                }
            }
        }
        return new ArrayList<>(undefinedConfigKeys);
    }

    private Set<String> getConfigsWithNoParent() {
        if (this.configsWithNoParent != null) {
            return this.configsWithNoParent;
        }
        Set<String> configsWithParent = new HashSet<>();

        for (ConfigKey configKey : configKeys.values()) {
            List<String> dependents = configKey.dependents;
            configsWithParent.addAll(dependents);
        }

        Set<String> configs = new HashSet<>(configKeys.keySet());
        configs.removeAll(configsWithParent);
        this.configsWithNoParent = configs;
        return configs;
    }

    private void parseForValidate(String name, Map<String, String> props, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);

        Object value = null;
        if (props.containsKey(key.name)) {
            try {
                value = parseType(key.name, props.get(key.name), key.type);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        } else if (key.defaultValue == NO_DEFAULT_VALUE) {
            config.addErrorMessage("Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            value = key.defaultValue;
        }

        if (key.validator != null) {
            try {
                key.validator.ensureValid(key.name, value);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }
        config.value(value);
        parsed.put(name, value);
        for (String dependent : key.dependents) {
            parseForValidate(dependent, props, parsed, configs);
        }
    }

    private void validate(String name, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);
        List<Object> recommendedValues;
        if (key.recommender != null) {
            try {
                recommendedValues = key.recommender.validValues(name, parsed);
                List<Object> originalRecommendedValues = config.recommendedValues();
                if (!originalRecommendedValues.isEmpty()) {
                    Set<Object> originalRecommendedValueSet = new HashSet<>(originalRecommendedValues);
                    Iterator<Object> it = recommendedValues.iterator();
                    while (it.hasNext()) {
                        Object o = it.next();
                        if (!originalRecommendedValueSet.contains(o)) {
                            it.remove();
                        }
                    }
                }
                config.recommendedValues(recommendedValues);
                config.visible(key.recommender.visible(name, parsed));
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }

        configs.put(name, config);
        for (String dependent : key.dependents) {
            validate(dependent, parsed, configs);
        }
    }

    /**
     * Parse a value according to its expected type.
     *
     * @param name The config name
     * @param value The config value
     * @param type The expected type
     * @return The parsed object
     */
    public static Object parseType(String name, Object value, Type type) {
        try {
            if (value == null) return null;

            String trimmed = null;
            if (value instanceof String) {
                trimmed = ((String) value).trim();
            }

            switch (type) {
                case BOOLEAN:
                    if (value instanceof String) {
                        if (trimmed.equalsIgnoreCase("true")) {
                            return true;
                        } else if (trimmed.equalsIgnoreCase("false")) {
                            return false;
                        } else {
                            throw new ConfigException(name, value, "Expected value to be either true or false");
                        }
                    } else if (value instanceof Boolean) {
                        return value;
                    } else {
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                    }
                case PASSWORD:
                    if (value instanceof Password) {
                        return value;
                    } else if (value instanceof String) {
                        return new Password(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                    }
                case STRING:
                    if (value instanceof String) {
                        return trimmed;
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                    }
                case INT:
                    if (value instanceof Integer) {
                        return (Integer) value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 32-bit integer, but it was a " + value.getClass().getName());
                    }
                case SHORT:
                    if (value instanceof Short) {
                        return (Short) value;
                    } else if (value instanceof String) {
                        return Short.parseShort(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 16-bit integer (short), but it was a " + value.getClass().getName());
                    }
                case LONG:
                    if (value instanceof Integer) {
                        return ((Integer) value).longValue();
                    }
                    if (value instanceof Long) {
                        return (Long) value;
                    } else if (value instanceof String) {
                        return Long.parseLong(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 64-bit integer (long), but it was a " + value.getClass().getName());
                    }
                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    } else if (value instanceof String) {
                        return Double.parseDouble(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a double, but it was a " + value.getClass().getName());
                    }
                case LIST:
                    if (value instanceof List) {
                        return (List<?>) value;
                    } else if (value instanceof String) {
                        if (trimmed.isEmpty()) {
                            return Collections.emptyList();
                        } else {
                            return Arrays.asList(trimmed.split("\\s*,\\s*", -1));
                        }
                    } else {
                        throw new ConfigException(name, value, "Expected a comma separated list.");
                    }
                case CLASS:
                    if (value instanceof Class) {
                        return (Class<?>) value;
                    } else if (value instanceof String) {
                        return Class.forName(trimmed, true, Utils.getContextOrKafkaClassLoader());
                    } else {
                        throw new ConfigException(name, value, "Expected a Class instance or class name.");
                    }
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(name, value, "Class " + value + " could not be found.");
        }
    }

    public static String convertToString(Object parsedValue, Type type) {
        if (parsedValue == null) {
            return null;
        }

        if (type == null) {
            return parsedValue.toString();
        }

        switch (type) {
            case BOOLEAN:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case STRING:
            case PASSWORD:
                return parsedValue.toString();
            case LIST:
                List<?> valueList = (List<?>) parsedValue;
                return Utils.join(valueList, ",");
            case CLASS:
                Class<?> clazz = (Class<?>) parsedValue;
                return clazz.getName();
            default:
                throw new IllegalStateException("Unknown type.");
        }
    }

    /**
     * The config types
     */
    public enum Type {
        BOOLEAN,
        STRING,
        INT,
        SHORT,
        LONG,
        DOUBLE,
        LIST,
        CLASS,
        PASSWORD
    }

    /**
     * The importance level for a configuration
     */
    public enum Importance {
        HIGH,
        MEDIUM,
        LOW
    }

    /**
     * The width of a configuration value
     */
    public enum Width {
        NONE,
        SHORT,
        MEDIUM,
        LONG
    }

    /**
     * This is used by the {@link #validate(Map)} to get valid values for a configuration given the current
     * configuration values in order to perform full configuration validation and visibility modification.
     * In case that there are dependencies between configurations, the valid values and visibility
     * for a configuration may change given the values of other configurations.
     */
    public interface Recommender {

        /**
         * The valid values for the configuration given the current configuration values.
         *
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The list of valid values. To function properly, the returned objects should have the type
         * defined for the configuration using the recommender.
         */
        List<Object> validValues(String name, Map<String, Object> parsedConfig);

        /**
         * Set the visibility of the configuration given the current configuration values.
         *
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The visibility of the configuration
         */
        boolean visible(String name, Map<String, Object> parsedConfig);
    }

    /**
     * Validation logic the user may provide to perform single configuration validation.
     */
    public interface Validator {
        /**
         * Perform single configuration validation.
         *
         * @param name The name of the configuration
         * @param value The value of the configuration
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges
     */
    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static Range atLeast(Number min) {
            return new Range(min, null);
        }

        /**
         * A numeric range that checks both the upper and lower bound
         */
        public static Range between(Number min, Number max) {
            return new Range(min, max);
        }

        @Override
        public void ensureValid(String name, Object o) {
            if (o == null) {
                throw new ConfigException(name, o, "Value must be non-null");
            }
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue()) {
                throw new ConfigException(name, o, "Value must be at least " + min);
            }
            if (max != null && n.doubleValue() > max.doubleValue()) {
                throw new ConfigException(name, o, "Value must be no more than " + max);
            }
        }

        @Override
        public String toString() {
            if (min == null) {
                return "[...," + max + "]";
            } else if (max == null) {
                return "[" + min + ",...]";
            } else {
                return "[" + min + ",...," + max + "]";
            }
        }
    }

    public static class ValidList implements Validator {

        ValidString validString;

        private ValidList(List<String> validStrings) {
            this.validString = new ValidString(validStrings);
        }

        public static ValidList in(String... validStrings) {
            return new ValidList(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(final String name, final Object value) {
            List<String> values = (List<String>) value;
            for (String string : values) {
                validString.ensureValid(name, string);
            }
        }

        @Override
        public String toString() {
            return validString.toString();
        }
    }

    public static class ValidString implements Validator {
        List<String> validStrings;

        private ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ValidString in(String... validStrings) {
            return new ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new ConfigException(name, o, "String must be one of: " + Utils.join(validStrings, ", "));
            }

        }

        @Override
        public String toString() {
            return "[" + Utils.join(validStrings, ", ") + "]";
        }
    }

    public static class ConfigKey {
        public final String name;
        public final Type type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;
        public final Importance importance;
        public final String group;
        public final int orderInGroup;
        public final Width width;
        public final String displayName;
        public final List<String> dependents;
        public final Recommender recommender;
        public final boolean internalConfig;

        public ConfigKey(String name, Type type, Object defaultValue, Validator validator,
                         Importance importance, String documentation, String group,
                         int orderInGroup, Width width, String displayName,
                         List<String> dependents, Recommender recommender,
                         boolean internalConfig) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
            this.validator = validator;
            this.importance = importance;
            if (this.validator != null && hasDefault()) {
                this.validator.ensureValid(name, this.defaultValue);
            }
            this.documentation = documentation;
            this.dependents = dependents;
            this.group = group;
            this.orderInGroup = orderInGroup;
            this.width = width;
            this.displayName = displayName;
            this.recommender = recommender;
            this.internalConfig = internalConfig;
        }

        public boolean hasDefault() {
            return this.defaultValue != NO_DEFAULT_VALUE;
        }
    }

    protected List<String> headers() {
        return Arrays.asList("Name", "Description", "Type", "Default", "Valid Values", "Importance");
    }

    protected String getConfigValue(ConfigKey key, String headerName) {
        switch (headerName) {
            case "Name":
                return key.name;
            case "Description":
                return key.documentation;
            case "Type":
                return key.type.toString().toLowerCase(Locale.ROOT);
            case "Default":
                if (key.hasDefault()) {
                    if (key.defaultValue == null) {
                        return "null";
                    }
                    String defaultValueStr = convertToString(key.defaultValue, key.type);
                    if (defaultValueStr.isEmpty()) {
                        return "\"\"";
                    } else {
                        return defaultValueStr;
                    }
                } else {
                    return "";
                }
            case "Valid Values":
                return key.validator != null ? key.validator.toString() : "";
            case "Importance":
                return key.importance.toString().toLowerCase(Locale.ROOT);
            default:
                throw new RuntimeException("Can't find value for header '" + headerName + "' in " + key.name);
        }
    }

    public String toHtmlTable() {
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        // print column headers
        for (String headerName : headers()) {
            b.append("<th>");
            b.append(headerName);
            b.append("</th>\n");
        }
        b.append("</tr>\n");
        for (ConfigKey def : configs) {
            if (def.internalConfig) {
                continue;
            }

            b.append("<tr>\n");
            // print column values
            for (String headerName : headers()) {
                b.append("<td>");
                b.append(getConfigValue(def, headerName));
                b.append("</td>");
            }
            b.append("</tr>\n");
        }
        b.append("</tbody></table>");
        return b.toString();
    }

    /**
     * Get the configs formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    public String toRst() {
        StringBuilder b = new StringBuilder();
        for (ConfigKey def : sortedConfigs()) {
            if (def.internalConfig) {
                continue;
            }
            getConfigKeyRst(def, b);
            b.append("\n");
        }
        return b.toString();
    }

    /**
     * Configs with new metadata (group, orderInGroup, dependents) formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    public String toEnrichedRst() {
        StringBuilder b = new StringBuilder();

        String lastKeyGroupName = "";
        for (ConfigKey def : sortedConfigs()) {
            if (def.internalConfig) {
                continue;
            }
            if (def.group != null) {
                if (!lastKeyGroupName.equalsIgnoreCase(def.group)) {
                    b.append(def.group).append("\n");
                    char[] underLine = new char[def.group.length()];
                    Arrays.fill(underLine, '^');
                    b.append(new String(underLine)).append("\n\n");
                }
                lastKeyGroupName = def.group;
            }

            getConfigKeyRst(def, b);

            if (def.dependents != null && def.dependents.size() > 0) {
                int j = 0;
                b.append("  * Dependents: ");
                for (String dependent : def.dependents) {
                    b.append("``");
                    b.append(dependent);
                    if (++j == def.dependents.size()) {
                        b.append("``");
                    } else {
                        b.append("``, ");
                    }
                }
                b.append("\n");
            }
            b.append("\n");
        }
        return b.toString();
    }

    /**
     * Shared content on Rst and Enriched Rst.
     */
    private void getConfigKeyRst(ConfigKey def, StringBuilder b) {
        b.append("``").append(def.name).append("``").append("\n");
        for (String docLine : def.documentation.split("\n")) {
            if (docLine.length() == 0) {
                continue;
            }
            b.append("  ").append(docLine).append("\n\n");
        }
        b.append("  * Type: ").append(getConfigValue(def, "Type")).append("\n");
        if (def.hasDefault()) {
            b.append("  * Default: ").append(getConfigValue(def, "Default")).append("\n");
        }
        if (def.validator != null) {
            b.append("  * Valid Values: ").append(getConfigValue(def, "Valid Values")).append("\n");
        }
        b.append("  * Importance: ").append(getConfigValue(def, "Importance")).append("\n");
    }

    /**
     * Get a list of configs sorted taking the 'group' and 'orderInGroup' into account.
     *
     * If grouping is not specified, the result will reflect "natural" order: listing required fields first, then ordering by importance, and finally by name.
     */
    private List<ConfigKey> sortedConfigs() {
        final Map<String, Integer> groupOrd = new HashMap<>(groups.size());
        int ord = 0;
        for (String group : groups) {
            groupOrd.put(group, ord++);
        }

        List<ConfigKey> configs = new ArrayList<>(configKeys.values());
        Collections.sort(configs, new Comparator<ConfigKey>() {
            @Override
            public int compare(ConfigKey k1, ConfigKey k2) {
                int cmp = k1.group == null
                        ? (k2.group == null ? 0 : -1)
                        : (k2.group == null ? 1 : Integer.compare(groupOrd.get(k1.group), groupOrd.get(k2.group)));
                if (cmp == 0) {
                    cmp = Integer.compare(k1.orderInGroup, k2.orderInGroup);
                    if (cmp == 0) {
                        // first take anything with no default value
                        if (!k1.hasDefault() && k2.hasDefault()) {
                            cmp = -1;
                        } else if (!k2.hasDefault() && k1.hasDefault()) {
                            cmp = 1;
                        } else {
                            cmp = k1.importance.compareTo(k2.importance);
                            if (cmp == 0) {
                                return k1.name.compareTo(k2.name);
                            }
                        }
                    }
                }
                return cmp;
            }
        });
        return configs;
    }

    public void embed(final String keyPrefix, final String groupPrefix, final int startingOrd, final ConfigDef child) {
        int orderInGroup = startingOrd;
        for (ConfigDef.ConfigKey key : child.sortedConfigs()) {
            define(new ConfigDef.ConfigKey(
                    keyPrefix + key.name,
                    key.type,
                    key.defaultValue,
                    embeddedValidator(keyPrefix, key.validator),
                    key.importance,
                    key.documentation,
                    groupPrefix + (key.group == null ? "" : ": " + key.group),
                    orderInGroup++,
                    key.width,
                    key.displayName,
                    embeddedDependents(keyPrefix, key.dependents),
                    embeddedRecommender(keyPrefix, key.recommender),
                    key.internalConfig));
        }
    }

    /**
     * Returns a new validator instance that delegates to the base validator but unprefixes the config name along the way.
     */
    private static ConfigDef.Validator embeddedValidator(final String keyPrefix, final ConfigDef.Validator base) {
        if (base == null) return null;
        return new ConfigDef.Validator() {
            @Override
            public void ensureValid(String name, Object value) {
                base.ensureValid(name.substring(keyPrefix.length()), value);
            }
        };
    }

    /**
     * Updated list of dependent configs with the specified {@code prefix} added.
     */
    private static List<String> embeddedDependents(final String keyPrefix, final List<String> dependents) {
        if (dependents == null) return null;
        final List<String> updatedDependents = new ArrayList<>(dependents.size());
        for (String dependent : dependents) {
            updatedDependents.add(keyPrefix + dependent);
        }
        return updatedDependents;
    }

    /**
     * Returns a new recommender instance that delegates to the base recommender but unprefixes the input parameters along the way.
     */
    private static ConfigDef.Recommender embeddedRecommender(final String keyPrefix, final ConfigDef.Recommender base) {
        if (base == null) return null;
        return new ConfigDef.Recommender() {
            private String unprefixed(String k) {
                return k.substring(keyPrefix.length());
            }

            private Map<String, Object> unprefixed(Map<String, Object> parsedConfig) {
                final Map<String, Object> unprefixedParsedConfig = new HashMap<>(parsedConfig.size());
                for (Map.Entry<String, Object> e : parsedConfig.entrySet()) {
                    if (e.getKey().startsWith(keyPrefix)) {
                        unprefixedParsedConfig.put(unprefixed(e.getKey()), e.getValue());
                    }
                }
                return unprefixedParsedConfig;
            }

            @Override
            public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                return base.validValues(unprefixed(name), unprefixed(parsedConfig));
            }

            @Override
            public boolean visible(String name, Map<String, Object> parsedConfig) {
                return base.visible(unprefixed(name), unprefixed(parsedConfig));
            }
        };
    }

}