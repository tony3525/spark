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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


object GridHITS {
  def apply(nRows: Int, nCols: Int, nIter: Int): Seq[(VertexId, Double)] = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outDegree = Array.fill(nRows * nCols)(0)
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r + 1, c)) += ind
      }
      if (c + 1 < nCols) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r, c + 1)) += ind
      }
    }
    // compute the authority score and hub score
    // var pr = Array.fill(nRows * nCols)(resetProb)
    var auth = Array.fill(nRows * nCols)(1.0)
    var hub = Array.fill(nRows * nCols)(1.0)
    for (iter <- 0 until nIter) {
      val oldAuth = auth
      val oldHub = hub
      // pr = new Array[Double](nRows * nCols)
      auth = new Array[Double](nRows*nCols)
      hub = new Array[Double](nRows*nCols)

      // Calculate the authority scores
      for (r <- 0 until nRows; c <- 0 until nCols) {
        val ind = sub2ind(r,c)
	if (r+1 < nRows) { auth(sub2ind(r+1,c)) += oldhub(sub2ind(r,c))}
	if (c+1 < nCols) { auth(sub2ind(r,c+1)) += oldhub(sub2ind(r,c))}
	      
      }
      // Calculate the norm
      val authNorm = 0
      for (ind <- 0 until (nRows * nCols)){
        authNorm += auth(ind) * auth(ind)
      }
      authNorm = math.sqrt(authNorm)

      // Scale the authority score
      for (ind <- 0 until (nRows * nCols)){
        auth(ind) /= authNorm
      }

      // Calculate the hub scores
      for (r <- 0 until nRows; c <- 0 until nCols) {
        val ind = sub2ind(r,c)
	if (r+1 < nRows) { hub(sub2ind(r,c)) += auth(sub2ind(r+1,c))}
	if (c+1 < nCols) { hub(sub2ind(r,c)) += auth(sub2ind(r,c+1))}      
      }
      // Calculate the norm
      val hubNorm = 0
      for (ind <- 0 until (nRows * nCols)){
        hubNorm += hub(ind) * hub(ind)
      }
      hubNorm = math.sqrt(hubNorm)

      // Scale the hub score
      for (ind <- 0 until (nRows * nCols)){
        hub(ind) /= hubNorm
      }

    }

  }

}


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  def compareScores(a: VertexRDD[(Double, Double)], b: VertexRDD[(Double, Double)]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a._1 - bOpt._1.getOrElse(0.0)) * (a._1 - bOpt._1.getOrElse(0.0)) + (a._2 - bOpt._2.getOrElse(0.0)) * (a._2 - bOpt._2.getOrElse(0.0)) }
      .map { case (id, error) => error }.sum()
  }

  test("Grid HITS") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val tol = 0.0001
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val pairs = gridGraph.staticHITS(numIter).vertices.cache()
      val dynamicRanks = gridGraph.pageRank(tol, resetProb).vertices.cache()
      val referencePairs = VertexRDD(
        sc.parallelize(GridHITS(rows, cols, numIter))).cache()

      assert(compareScores(pairs, referencePairs) < errorTol)
      
    }
  } // end of Grid PageRank
  
}
