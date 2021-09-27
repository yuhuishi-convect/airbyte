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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.config.Configs;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class V0_29_21_001__Store_last_sync_state_test extends AbstractConfigsDatabaseTest {

  @Test
  @Order(1)
  public void testGetJobsDatabase() {
    final Configs configs = mock(Configs.class);

    // when there is no database environment variable, the return value is empty
    assertTrue(V0_29_21_001__Store_last_sync_state.getJobsDatabase().isEmpty());

    // when there is database environment variable, return the database
    when(configs.getDatabaseUser()).thenReturn(container.getUsername());
    when(configs.getDatabasePassword()).thenReturn(container.getPassword());
    when(configs.getDatabaseUrl()).thenReturn(container.getJdbcUrl());
  }

  @Test
  @Order(2)
  public void testGetSyncToStateMap() {

  }

  @Test
  @Order(3)
  public void testCreateTable() {

  }

  @Test
  @Order(4)
  public void testCopyData() {

  }

}
