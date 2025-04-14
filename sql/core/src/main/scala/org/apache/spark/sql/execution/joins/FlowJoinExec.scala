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

package org.apache.spark.sql.execution.joins

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BoundReference, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.CatalystSerde
import org.apache.spark.sql.execution.{ExternalRDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

case class FlowJoinExec(
  leftKeys: Seq[Expression],
  rightKeys: Seq[Expression],
  joinType: JoinType,
  brBuildSide: BuildSide,
  shBuildSide: BuildSide,
  condition: Option[Expression],
  left: SparkPlan,
  right: SparkPlan,
  heavyHitters: Seq[RDD[UnsafeRow]] = Seq.empty
) extends BaseJoinExec {

  override def output: Seq[Attribute] = left.output ++ right.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private def bindReference(
    expressions: Seq[Expression],
    input: AttributeSeq
  ): BoundReference =
    expressions match {
      case Seq(a: AttributeReference) =>
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          throw SparkException.internalError(
            s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}"
          )
        } else {
          BoundReference(ordinal, a.dataType, input(ordinal).nullable)
        }
    }

  private def createPlan(table: DataFrame): SparkPlan =
    ExternalRDDScanExec(CatalystSerde.generateObjAttr[Row](Encoders.row(table.schema)), table.rdd)

  private def split[T](
    table: DataFrame,
    col: BoundReference,
    heavyHitters: Set[T]
  ): (SparkPlan, SparkPlan) = {
    val idx = col.ordinal
    (
      createPlan(table.filter(row => heavyHitters(row.getAs[T](idx)))),
      createPlan(table.filter(row => !heavyHitters(row.getAs[T](idx))))
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val sparkSession = left.session

    val leftResults = sparkSession.createDataFrame(
      left.executeCollectPublic().toList.asJava,
      left.schema
    )
    val rightResults = sparkSession.createDataFrame(
      right.executeCollectPublic().toList.asJava,
      right.schema
    )

    val leftKeyReference = bindReference(leftKeys, left.output)
    val rightKeyReference = bindReference(rightKeys, right.output)

    // TODO load from files
    // TODO remove Any
    val heavyHitters = Set.empty[Any]

    val (leftBr, leftSc) = split[Any](leftResults, leftKeyReference, heavyHitters)
    val (rightBr, rightSc) = split[Any](rightResults, rightKeyReference, heavyHitters)

    val broadcastHashJoinExec = BroadcastHashJoinExec(
      leftKeys, rightKeys, joinType, brBuildSide, condition, leftBr, rightBr
    )
    val shuffledHashJoinExec = ShuffledHashJoinExec(
      leftKeys, rightKeys, joinType, shBuildSide, condition, leftSc, rightSc, isSkewJoin = false
    )

    val broadcastHashJoinResults = sparkSession.createDataFrame(
      broadcastHashJoinExec.executeCollectPublic().toList.asJava,
      broadcastHashJoinExec.schema
    )
    val shuffledHashJoinResults = sparkSession.createDataFrame(
      shuffledHashJoinExec.executeCollectPublic().toList.asJava,
      shuffledHashJoinExec.schema
    )
    createPlan(broadcastHashJoinResults.union(shuffledHashJoinResults)).execute()
  }

  override protected def withNewChildrenInternal(
    newLeft: SparkPlan, newRight: SparkPlan): FlowJoinExec =
    copy(left = newLeft, right = newRight)
}
