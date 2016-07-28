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

package org.apache.spark.graphx.lib

import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

/**
 * Implement Hyperlink-Induced Topic Search algorithm
 * available at [[http://www.cs.cornell.edu/home/kleinber/auth.pdf]]
 */

object HITS extends Logging {


  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing hub score and authority score
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph containing with each vertex containing
   *         the authority score and hub score
   *
   */

  def runWithOptions[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), ED] =
  {
    require(numIter > 0, s"Number of iterations must be greater than 0," + s"but got ${numIter}")

    // Initialize the HITS graph with each vertex attribute having
    // authority score 1.0 and hub score 1.0.  Edge attribute is not needed

    var hitsGraph : Graph[(Double, Double), ED] = graph
    // Set the vertex attributes to the initial value:
    // authority = 1.0, hub = 1.0
      .mapVertices { (id, attr) => (1.0, 1.0)}
    var iteration = 0

    var prevHitsGraph: Graph[(Double, Double), ED] = null
    while (iteration < numIter) {
      hitsGraph.cache()

      prevHitsGraph = hitsGraph
      // add up the hub scores of sources of incoming edges
      val authUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src)
      hitsGraph = hitsGraph.outerJoinVertices(authUpdates) {
        (id, oldValue, msgSumOpt) => (msgSumOpt.getOrElse(0.0), oldValue._2)
      }.cache()

      // add up the authority scores of destinations of outgoing edges
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _)
      hitsGraph = hitsGraph.outerJoinVertices(hubUpdates) {
        (id, oldValue, msgSumOpt) => (oldValue._1, msgSumOpt.getOrElse(0.0))
      }

      logInfo(s"HITS finished iteration $iteration.")
      prevHitsGraph.vertices.unpersist(false)
      prevHitsGraph.edges.unpersist(false)

      iteration += 1
    }
    // calculate the norm of authority scores
    val squareAuth = hitsGraph.vertices.map(v => v._2._1 * v._2._1).sum()
    val normAuth = math.sqrt(squareAuth)

    // calculate the norm of hub scores
    val squareHub = hitsGraph.vertices.map(v => v._2._2 * v._2._2).reduce((a, b) => a + b)
    val normHub = math.sqrt(squareHub)

    // normalize authority and hub scores
    hitsGraph = hitsGraph.mapVertices{ (id, attr) => (attr._1 / normAuth, attr._2 / normHub)}

    hitsGraph
  }

}
