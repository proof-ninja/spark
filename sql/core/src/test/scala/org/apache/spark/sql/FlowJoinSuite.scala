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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FlowJoinSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
  with JoinSelectionHelper {
  import testImplicits._

  setupTestData()

  test("inner join where, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N"),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")
        ))
    }
  }

//  test("FlowJoinTest") {
  //
  //    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
  //      val eq = $"n" === $"N"
  //
  //      // scalastyle:off
  //      val fn = eq.node.asInstanceOf[internal.UnresolvedFunction]
  //      println("functionName = ", fn.functionName)
  //      println("Arguments = ", fn.arguments)
  //      val Seq(left, right) = fn.arguments
  //      val leftAttr = left.asInstanceOf[internal.UnresolvedAttribute]
  //      println("Left = ", leftAttr.nameParts.head)
  //      val rightAttr = right.asInstanceOf[internal.UnresolvedAttribute]
  //      println("Right = ", rightAttr.nameParts.head)
  //      // scalastyle:on
  //
  //      assert(true)
  //    }
  //  }

  test("FlowJoin") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        upperCaseData.flowJoin(lowerCaseData, $"N" === $"n", Set(1, 3)),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")
        ))
    }
  }
}
