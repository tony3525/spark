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






class HITSSuite extends SparkFunSuite with LocalSparkContext {

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val scores1 = starGraph.staticHITS(numIter = 1).vertices
      val scores2 = starGraph.staticHITS(numIter = 10).vertices.cache()

      // Static HITS should only take 1 iterations to converge.
      // The scores from step 1 and step 10 should be the same
      val notMatching = scores1.innerZipJoin(scores2) { (vid, pr1, pr2) =>
        if (math.abs(pr1._1 - pr2._1) > 1.0E-10 || math.abs(pr1._2 - pr2._2) > 1.0E-10) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      // The authority-hub score pairs after any number of iterations should be
      // vertex 0 (center) : (1.0, 0.0)
      // vertex i (i > 0)  : (0.0, 1.0/sqrt(nVertices-1))
      val staticErrors = scores2.map { case (vid, pr) =>
        val correct = (vid > 0 && math.abs(pr._1) < 1.0E-10
          && math.abs(pr._2 - math.sqrt(1.0/(nVertices - 1))) < 1.0E-10) ||
          (vid == 0L && math.abs(pr._1 - 1.0) < 1.0E-10 && math.abs(pr._2) < 1.0E-10)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0)

    }
  } // end of test Star HITS
}
