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
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec, OreOreJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CustomJoinSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
  with JoinSelectionHelper {
  import testImplicits._

  setupTestData()

  def statisticSizeInByte(df: DataFrame): BigInt = {
    df.queryExecution.optimizedPlan.stats.sizeInBytes
  }

  test("equi-join is hash-join") {
    val x = testData2.as("x")
    val y = testData2.as("y")
    val join = x.join(y, $"x.a" === $"y.a", "inner").queryExecution.optimizedPlan
    val planned = spark.sessionState.planner.JoinSelection(join)
    assert(planned.size === 1)
    assert(planned.head.isInstanceOf[OreOreJoinExec])
  }

  def assertJoin(pair: (String, Class[_ <: BinaryExecNode])): Any = {
    val sqlString = pair._1
    val c = pair._2
    val df = sql(sqlString)
    val optimized = df.queryExecution.optimizedPlan
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: BroadcastHashJoinExec => j
      case j: ShuffledHashJoinExec => j
      case j: CartesianProductExec => j
      case j: BroadcastNestedLoopJoinExec => j
      case j: SortMergeJoinExec => j
    }

    assert(operators.size === 1)
    if (operators.head.getClass != c) {
      fail(s"$sqlString expected operator: $c, but got ${operators.head}\n physical: \n$physical")
    }
    assert(
      canPlanAsBroadcastHashJoin(optimized.asInstanceOf[Join], conf) ===
        operators.head.isInstanceOf[BroadcastHashJoinExec],
      "canPlanAsBroadcastHashJoin not in sync with join selection codepath!")
    operators.head
  }

//  test("broadcastable join with shuffle join hint") {
//    spark.sharedState.cacheManager.clearCache()
//    sql("CACHE TABLE testData")
//    // Make sure it's planned as broadcast join without the hint.
//    assertJoin("SELECT * FROM testData JOIN testData2 ON key = a",
//      classOf[BroadcastHashJoinExec])
//    assertJoin("SELECT /*+ SHUFFLE_HASH(testData) */ * FROM testData JOIN testData2 ON key = a",
//      classOf[ShuffledHashJoinExec])
//  }

  test("inner join where, one match per row") {
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
