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
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


class FlowJoinSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
  with JoinSelectionHelper {
  import testImplicits._

  setupTestData()

  def statisticSizeInByte(df: classic.DataFrame): BigInt = {
    df.queryExecution.optimizedPlan.stats.sizeInBytes
  }

  test("flow join from hint") {
    withSQLConf(SQLConf.FORCE_FLOW_JOIN_EXEC.key -> "false") {
      spark.sharedState.cacheManager.clearCache()
      val df = testData.hint("flow_join").join(testData2, $"key" === $"a")
      val physical = df.queryExecution.sparkPlan
      val operators = physical.collect {
        case j: BroadcastHashJoinExec => j
        case j: ShuffledHashJoinExec => j
        case j: CartesianProductExec => j
        case j: BroadcastNestedLoopJoinExec => j
        case j: SortMergeJoinExec => j
        case j: FlowJoinExec => j
      }

      assert(operators.size === 1)
      assert(operators.head.getClass === classOf[FlowJoinExec])
    }
  }

  test("flow join from hint 2") {
    withSQLConf(SQLConf.FORCE_FLOW_JOIN_EXEC.key -> "false") {
      spark.sharedState.cacheManager.clearCache()
      val df = testData.join(testData2.hint("flow_join"), $"key" === $"a")
      val physical = df.queryExecution.sparkPlan
      val operators = physical.collect {
        case j: BroadcastHashJoinExec => j
        case j: ShuffledHashJoinExec => j
        case j: CartesianProductExec => j
        case j: BroadcastNestedLoopJoinExec => j
        case j: SortMergeJoinExec => j
        case j: FlowJoinExec => j
      }

      assert(operators.size === 1)
      assert(operators.head.getClass === classOf[FlowJoinExec])
    }
  }
}
