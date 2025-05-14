/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.FlowJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FlowJoinExecSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
  with JoinSelectionHelper {

  import testImplicits._

  setupTestData()

  test("equi-join is flow-join") {
    withSQLConf(SQLConf.FORCE_FLOW_JOIN_EXEC.key -> "true") {
      val x = testData2.as("x")
      val y = testData2.as("y")

      val join = x.join(y, $"x.a" === $"y.a", "inner").queryExecution.optimizedPlan
      val planned = spark.sessionState.planner.JoinSelection(join)
      assert(planned.size === 1)
      assert(planned.head.isInstanceOf[FlowJoinExec])
    }
  }

  // ==== failed ====
  // org.apache.spark.SparkUnsupportedOperationException:
  // Scan does not implement doExecuteBroadcast.
  test("inner join where, one match per row") {
    withSQLConf(SQLConf.FORCE_FLOW_JOIN_EXEC.key -> "true") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        checkAnswer(
          upperCaseData.join(lowerCaseData).where($"n" === $"N"),
          Seq(
            Row(1, "A", 1, "a"),
            Row(2, "B", 2, "b"),
            Row(3, "C", 3, "c"),
            Row(4, "D", 4, "d")
          ))
      }
    }
  }
}
