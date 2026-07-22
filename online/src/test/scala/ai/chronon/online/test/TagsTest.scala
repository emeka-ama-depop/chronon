/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.test

import ai.chronon.api.Builders
import ai.chronon.online.metrics.Metrics.Environment
import ai.chronon.online.metrics.{Metrics, OtelMetricsReporter}
import io.opentelemetry.api.OpenTelemetry
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class TagsTest extends AnyFlatSpec {
  // test that ttlCache of context is creates non duplicated entries

  it should "cached tags are computed tags" in {
    val otelMetricsClient = new OtelMetricsReporter(OpenTelemetry.noop())
    val context = Metrics.Context(
      Environment.JoinOffline,
      Builders.Join(
        metaData = Builders.MetaData("join1", team = "team1"),
        left = Builders.Source.events(
          query = null,
          table = "table1"
        ),
        joinParts = Builders.JoinPart(
          groupBy = Builders.GroupBy(
            metaData = Builders.MetaData("group_by1", team = "team2"),
            sources = Seq(
              Builders.Source.events(
                query = null,
                table = "table2"
              )
            )
          )
        ) :: Nil
      )
    )
    val copyFake = context.copy(join = "something else")
    val copyCorrect = copyFake.copy(join = context.join)

    // add three entries to cache - two distinct contexts and one copy of the first
    otelMetricsClient.tagCache(context)
    otelMetricsClient.tagCache(copyCorrect)
    otelMetricsClient.tagCache(copyFake)
    assertEquals(otelMetricsClient.tagCache.cMap.size(), 2)

    val slowTags = otelMetricsClient.tagCache(context)
    val fastTags = otelMetricsClient.tagCache(copyCorrect)
    assertEquals(slowTags, fastTags)
  }

  it should "include branch tag only when set" in {
    val withBranch = Metrics.Context(Environment.JoinFetching, branch = "release/1.2")
    assertEquals("release/1.2", withBranch.toTags(Metrics.Tag.Branch))

    val withoutBranch = Metrics.Context(Environment.JoinFetching)
    assertEquals(false, withoutBranch.toTags.contains(Metrics.Tag.Branch))
  }

}
