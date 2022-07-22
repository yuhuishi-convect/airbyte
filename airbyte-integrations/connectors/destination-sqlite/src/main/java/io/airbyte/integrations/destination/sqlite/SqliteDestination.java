/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.sqlite;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.airbyte.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcUtils;

public class SqliteDestination extends AbstractJdbcDestination implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteDestination.class);
  public static final String DRIVER_CLASS = DatabaseDriver.SQLITE.getDriverClassName();

  public static void main(String[] args) throws Exception {
    new IntegrationRunner(new SqliteDestination()).run(args);
  }

  public SqliteDestination() {
    
  }


  @Override
  protected Map<String, String> getDefaultConnectionProperties(final JsonNode config) {
    return Collections.emptyMap();
  }

  @Override
  public JsonNode toJdbcConfig(final JsonNode config) {
    final String jdbcUrl = String.format(
      "jdbc:sqlite:%s", 
      config.get("destination_path")
    );

    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
      .put(JdbcUtils.JDBC_URL_KEY, jdbcUrl);

    return Jsons.jsonNode(configBuilder.build());
  }



}
