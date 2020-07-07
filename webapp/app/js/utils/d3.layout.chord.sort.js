/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

customChordLayout = function () {
  var e = 1e-6, e2 = e * e, PI = Math.PI, doublePI = 2 * PI, doublePIe = doublePI - e, halfPI = PI / 2,
    d3_radians = PI / 180, d3_degrees = 180 / PI;
  var chord = {}, chords, groups, matrix, n, padding = 0, sortGroups, sortSubgroups, sortChords;

  function relayout() {
    var subgroups = {}, groupSums = [], groupIndex = d3.range(n), subgroupIndex = [], k, x, x0, i, j;
    chords = [];
    groups = [];
    k = 0, i = -1;
    while (++i < n) {
      x = 0, j = -1;
      while (++j < n) {
        x += matrix[i][j];
      }
      groupSums.push(x);
      subgroupIndex.push(d3.range(n).reverse());
      k += x;
    }
    if (sortGroups) {
      groupIndex.sort(function (a, b) {
        return sortGroups(groupSums[a], groupSums[b]);
      });
    }
    if (sortSubgroups) {
      subgroupIndex.forEach(function (d, i) {
        d.sort(function (a, b) {
          return sortSubgroups(matrix[i][a], matrix[i][b]);
        });
      });
    }
    k = (doublePI - padding * n) / k;
    x = 0, i = -1;
    while (++i < n) {
      x0 = x, j = -1;
      while (++j < n) {
        var di = groupIndex[i], dj = subgroupIndex[di][j], v = matrix[di][dj], a0 = x, a1 = x += v * k;
        subgroups[di + "-" + dj] = {
          index: di,
          subindex: dj,
          startAngle: a0,
          endAngle: a1,
          value: v
        };
      }
      groups[di] = {
        index: di,
        startAngle: x0,
        endAngle: x,
        value: (x - x0) / k
      };
      x += padding;
    }
    i = -1;
    while (++i < n) {
      j = i - 1;
      while (++j < n) {
        var source = subgroups[i + "-" + j], target = subgroups[j + "-" + i];
        if (source.value || target.value) {
          chords.push(source.value < target.value ? {
            source: target,
            target: source
          } : {
            source: source,
            target: target
          });
        }
      }
    }
    if (sortChords) resort();
  }

  function resort() {
    chords.sort(function (a, b) {
      return sortChords((a.source.value + a.target.value) / 2, (b.source.value + b.target.value) / 2);
    });
  }

  chord.matrix = function (x) {
    if (!arguments.length) return matrix;
    n = (matrix = x) && matrix.length;
    chords = groups = null;
    return chord;
  };
  chord.padding = function (x) {
    if (!arguments.length) return padding;
    padding = x;
    chords = groups = null;
    return chord;
  };
  chord.sortGroups = function (x) {
    if (!arguments.length) return sortGroups;
    sortGroups = x;
    chords = groups = null;
    return chord;
  };
  chord.sortSubgroups = function (x) {
    if (!arguments.length) return sortSubgroups;
    sortSubgroups = x;
    chords = null;
    return chord;
  };
  chord.sortChords = function (x) {
    if (!arguments.length) return sortChords;
    sortChords = x;
    if (chords) resort();
    return chord;
  };
  chord.chords = function () {
    if (!chords) relayout();
    return chords;
  };
  chord.groups = function () {
    if (!groups) relayout();
    return groups;
  };
  return chord;
};
