/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.db.instance.configs.migrations;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.db.Database;
import io.airbyte.db.instance.jobs.JobsDatabaseInstance;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record2;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a new table to store the latest job state for each standard sync. Issue:
 */
public class V0_29_21_001__Store_last_sync_state extends BaseJavaMigration {

  private static final String MIGRATION_NAME = "Configs db migration 0.29.21.001";
  private static final Logger LOGGER = LoggerFactory.getLogger(V0_29_21_001__Store_last_sync_state.class);

  private static final Table<?> TABLE = DSL.table("latest_sync_state");
  private static final Field<UUID> COLUMN_SYNC_ID = DSL.field("sync_id", SQLDataType.UUID.nullable(false));
  private static final Field<JSONB> COLUMN_STATE = DSL.field("state", SQLDataType.JSONB.nullable(false));
  private static final Field<OffsetDateTime> COLUMN_CREATED_AT = DSL.field("created_at",
      SQLDataType.OFFSETDATETIME.nullable(false).defaultValue(DSL.currentOffsetDateTime()));
  private static final Field<OffsetDateTime> COLUMN_UPDATED_AT = DSL.field("updated_at",
      SQLDataType.OFFSETDATETIME.nullable(false).defaultValue(DSL.currentOffsetDateTime()));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    final Configs configs = new EnvConfigs();
    createTable(ctx);
    copyData(ctx, getSyncToStateMap(configs));
  }

  @VisibleForTesting
  static void createTable(final DSLContext ctx) {
    ctx.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";");
    ctx.createTable(TABLE)
        .column(COLUMN_SYNC_ID)
        .column(COLUMN_STATE)
        .column(COLUMN_CREATED_AT)
        .column(COLUMN_UPDATED_AT)
        .execute();
    ctx.createUniqueIndexIfNotExists(String.format("%s_sync_id_idx", TABLE))
        .on(TABLE, Collections.singleton(COLUMN_SYNC_ID))
        .execute();
  }

  @VisibleForTesting
  static void copyData(final DSLContext ctx, final Map<String, JsonNode> syncToStateMap) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    for (Map.Entry<String, JsonNode> entry : syncToStateMap.entrySet()) {
      ctx.insertInto(TABLE)
          .set(COLUMN_SYNC_ID, UUID.fromString(entry.getKey()))
          .set(COLUMN_STATE, JSONB.valueOf(Jsons.serialize(entry.getValue())))
          .set(COLUMN_CREATED_AT, timestamp)
          .set(COLUMN_UPDATED_AT, timestamp)
          .execute();
    }
  }

  /**
   * This migration requires a connection to the job database, which may be a separate database from
   * the config database. However, the job database only exists in production, not in development or
   * test. We use the job database environment variables to determine how to connect to the job
   * database. This approach is not 100% reliable. However, it is better than doing half of the
   * migration here (creating the table), and the rest of the work during server start up (copying the
   * data from the job database).
   */
  @VisibleForTesting
  static Optional<Database> getJobsDatabase(final Configs configs) {
    try {
      // If the environment variables exist, it means the migration is run in production.
      // Connect to the official job database.
      final Database jobsDatabase = new JobsDatabaseInstance(
          configs.getDatabaseUser(),
          configs.getDatabasePassword(),
          configs.getDatabaseUrl())
              .getInitialized();
      LOGGER.info("[{}] Connected to jobs database: {}", MIGRATION_NAME, configs.getDatabaseUrl());
      return Optional.of(jobsDatabase);
    } catch (final IllegalArgumentException e) {
      // If the environment variables do not exist, it means the migration is run in development.
      // Connect to a mock job database, because we don't need to copy any data in test.
      LOGGER.info("[{}] This is the dev environment; there is no jobs database", MIGRATION_NAME);
      return Optional.empty();
    } catch (final IOException e) {
      throw new RuntimeException("Cannot connect to jobs database", e);
    }
  }

  /**
   * @return a map from sync id to last job attempt state.
   */
  @VisibleForTesting
  static Map<String, JsonNode> getSyncToStateMap(final Configs configs) throws SQLException {
    final Optional<Database> jobsDatabase = getJobsDatabase(configs);
    if (jobsDatabase.isEmpty()) {
      return Collections.emptyMap();
    }

    final Field<String> syncIdField = DSL.field("jobs.scope", SQLDataType.VARCHAR).as("sync_id");
    final Field<OffsetDateTime> attemptUpdateField = DSL.field("attempts.updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
    final Field<JSONB> attemptStateField = DSL.field("attempts.output -> 'sync' -> 'state'", SQLDataType.JSONB).as("attempt_state");

    return jobsDatabase.get().query(ctx -> ctx
        .selectDistinct(syncIdField, attemptStateField)
        .on(syncIdField, attemptUpdateField)
        .from(DSL.table("jobs"))
        .leftJoin(DSL.table("attempts"))
        .on(DSL.field("jobs.id").eq(DSL.field("attempts.jobs_id")))
        .where(attemptStateField.isNotNull())
        .orderBy(syncIdField.asc(), attemptUpdateField.desc())
        .fetch()
        .stream().collect(Collectors.toMap(
            Record2::value1,
            r -> Jsons.deserialize(r.value2().data()))));
  }

}
