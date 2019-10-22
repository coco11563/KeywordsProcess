package pub.sha0w.ETL.Objects.Trie

import pub.sha0w.ETL.Objects.HierarchyKeyword

import scala.collection.mutable

class TrieNode (val ApplyCode : String) extends Serializable {

  val keywordF : mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

  val subordinateCodeList : mutable.ListBuffer[TrieNode] = new mutable.ListBuffer[TrieNode]

  def apply(ApplyCode: String): TrieNode = new TrieNode(ApplyCode)

  def addHierarchyKeyword(hk : HierarchyKeyword) : Unit = {

  }

  def findHierarchyKeywordF(hk : HierarchyKeyword) : Int = ???
}

