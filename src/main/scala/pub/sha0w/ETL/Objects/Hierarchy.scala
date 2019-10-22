package pub.sha0w.ETL.Objects

/**
  * Here are four common pitfalls2
  * that can cause inconsistent behavior when
  * overriding equals:
  * 1. Defining equals with the wrong signature.
  * 2. Changing equals without also changing hashCode.
  * 3. Defining equals in terms of mutable fields.
  * 4. Failing to define equals as an equivalence relation.
  *
  * @param FOS
  * @param ApplyID
  */
class Hierarchy (val FOS: String, val ApplyID : String)extends Serializable {
  assert(ApplyID != null, s"this hierarchy's apply id is null, illegal form, fos is $FOS")
  override def hashCode(): Int = {
    if (FOS == null) ApplyID.hashCode else
    (FOS + ApplyID).hashCode
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) false
    else {
      if (!obj.isInstanceOf[Hierarchy]) {
        false
      } else {
        val tmp = obj.asInstanceOf[Hierarchy]
        if (tmp.FOS == null && this.FOS == null) {
          if (tmp.ApplyID == this.ApplyID) true else false
        } else if (tmp.ApplyID == this.ApplyID && tmp.FOS == this.FOS) true else false
      }
    }
  }

  override def clone(): AnyRef = {
    new Hierarchy(FOS, ApplyID)
  }

  def isNullAtFos : Boolean ={
    FOS == null
  }
  override def toString: String = {
    s"FOS : $FOS , ApplyID : $ApplyID"
  }
}

class HierarchyKeyword (val keyword: String, val hierarchy: Hierarchy) extends Serializable {
  override def hashCode(): Int = {
    if (hierarchy.ApplyID == null) {
      (keyword + hierarchy.ApplyID).hashCode
    } else (keyword + hierarchy.FOS + hierarchy.ApplyID).hashCode
  }
  def hasROS : Boolean = hierarchy.isNullAtFos
  override def equals(obj: Any): Boolean = {
    if (obj == null) false
    else {
      if (!obj.isInstanceOf[HierarchyKeyword]) {
        false
      } else {
        val tmp = obj.asInstanceOf[HierarchyKeyword]
        if (tmp.hierarchy.equals( this.hierarchy) &&
        tmp.keyword == this.keyword ) true
        else false
      }
    }
  }

  override def clone(): AnyRef = {
    new HierarchyKeyword(keyword, new Hierarchy(this.hierarchy.FOS, this.hierarchy.ApplyID))
  }

  override def toString: String = {
    s"keyword info => \n" +
      s"name : ${this.keyword} , FOS : ${this.hierarchy.FOS} , ApplyID : ${this.hierarchy.ApplyID}"
  }

  def toSimpleHash : Int = {
    (this.keyword + hierarchy.FOS).hashCode
  }
}