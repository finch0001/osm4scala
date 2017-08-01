/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Ángel Cervera Claudio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.acervera.osm4scala.model

import org.openstreetmap.osmosis.osmbinary.osmformat.{Relation, StringTable}


/**
  * Entity that represent a OSM relation as https://wiki.openstreetmap.org/wiki/Elements#Relation and https://wiki.openstreetmap.org/wiki/Relation describe
  */
case class RelationEntity(val id: Long, val relations: Seq[RelationMemberEntity], val tags: Map[String, String]) extends OSMEntity {

  override val osmModel: OSMTypes.Value = OSMTypes.Relation

}

object RelationEntity {

  def apply(osmosisStringTable: StringTable, osmosisRelation: Relation) = {

    // Calculate tags using the StringTable.
    val tags = (osmosisRelation.keys, osmosisRelation.vals).zipped.map { (k, v) => osmosisStringTable.s(k).toString("UTF-8") -> osmosisStringTable.s(v).toString("UTF-8") }.toMap

    // Decode members references in stored in delta compression.
    val members = osmosisRelation.memids.scanLeft(0l) { _ + _ }.drop(1)

    // Calculate relations
    val relations = (members, osmosisRelation.types, osmosisRelation.rolesSid).zipped.map { (m,t,r) => RelationMemberEntity(m,RelationMemberEntityTypes(t.value),osmosisStringTable.s(r).toString("UTF-8")) }

    new RelationEntity(osmosisRelation.id, relations, tags)
  }

}

