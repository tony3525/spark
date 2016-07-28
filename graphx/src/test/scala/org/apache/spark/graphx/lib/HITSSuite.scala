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
import org.apache.spark.rdd.RDD

class HITSSuite extends SparkFunSuite with LocalSparkContext {

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val tolerance = 1E-12
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val scores1 = starGraph.staticHITS(numIter = 1).vertices
      val scores2 = starGraph.staticHITS(numIter = 2).vertices.cache()

      // Static HITS should only take 1 iterations to converge.
      // The scores from step 1 and step 10 should be the same
      val notMatching = scores1.innerZipJoin(scores2) { (vid, pr1, pr2) =>
        if (math.abs(pr1._1 - pr2._1) > tolerance || math.abs(pr1._2 - pr2._2) > tolerance) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0, "The HITS algorithm fails to converge in one step.")

      // The authority-hub score pairs after any number of iterations should be
      // vertex 0 (center) : (1.0, 0.0)
      // vertex i (i > 0)  : (0.0, 1.0/sqrt(nVertices-1))
      val staticErrors = scores2.map { case (vid, pr) =>
        val correct = (vid > 0 && math.abs(pr._1) < tolerance
          && math.abs(pr._2 - math.sqrt(1.0/(nVertices - 1))) < tolerance) ||
          (vid == 0L && math.abs(pr._1 - 1.0) < tolerance && math.abs(pr._2) < tolerance)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0, "The HITS algorithm converges to a wrong solution.")

    }
  } // end of test Star HITS



  test("Grid HITS") {
    withSpark { sc =>
      val rows = 3
      val cols = 3
      val tolerance = 1E-3
      val numIter = 10
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()
      val scores = gridGraph.staticHITS(numIter).vertices
      val referenceScores: VertexRDD[(Double, Double)] = VertexRDD(
        sc.parallelize(Array((0L, (0.0, 0.0080)), (1L, (0.0070, 0.4629)), (2L, (0.2672, 0.3086)),
          (3L, (0.0070, 0.4629)), (4L, (0.5345, 0.6172)), (5L, (0.5344, 0.0080)),
          (6L, (0.2672, 0.3086)), (7L, (0.5344, 0.0080)), (8L, (0.0139, 0.0)))))

      // The scores from step 1 and step 10 should be the same
      val notMatching = scores.innerZipJoin(referenceScores) { (vid, pr1, pr2) =>
        if (math.abs(pr1._1 - pr2._1) > tolerance || math.abs(pr1._2 - pr2._2) > tolerance) 1 else 0
      }.map { case (vid, test) => test }.sum()

      assert(notMatching === 0, "The HITS algorithm fails to converge.")

    }
  } // end of test Grid HITS

  test("Loop HITS") {
    withSpark { sc =>
      val nVertices = 10
      val tolerance = 1E-12
      val numIter = 10
      val edges: RDD[(VertexId, VertexId)] =
        sc.parallelize(1 until nVertices).map(vid => (vid, (vid - 1) % nVertices + 1))
      // The graph forms a loop
      val loopGraph = Graph.fromEdgeTuples(edges, 1)
      // The HITS algorithm converges in one step.
      val scores = loopGraph.staticHITS(numIter).vertices
      val sol = 1.0 / 3
      // Check the correctness of the solution
      val nonMatching = scores.map{ case (vid, scores) =>
        if (math.abs(scores._1 - sol) > tolerance || math.abs(scores._2 - sol) > tolerance) 1 else 0
        }.sum()
      assert(nonMatching === 0, "The HITS algorithm fails to converge.")

    }
  } // end of test Loop HITS
}
