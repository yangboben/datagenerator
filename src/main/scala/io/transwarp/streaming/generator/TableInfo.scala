package io.transwarp.streaming.generator

import scala.util.Random

/**
  * Created by shentao on 16-2-18.
  */
class TableInfo(val tablename:String,val linenumber:Int) {

    var rowinfos:List[GeneratorRowInfo]=List()




    def addRow(rowname:String, rowtype:Int): Unit ={
     rowinfos=ROWTYPE.create_rowinfo(rowinfos.length,rowname,rowtype)::rowinfos
    }

    def addRow(row:String): Unit={
      val rowinfo=row.split(" ").filter(_.length>0).map(_.trim).map(_.toLowerCase)
      if(rowinfo.length==2){
        addRow(rowinfo(0),ROWTYPE.get_rowtype(rowinfo(1)))
      }
    }

    def getRownumber(rowname:String):Int={
      //返回-1表示无这一列
      //return 的是这一列在list中的下标
      for(e <- rowinfos){
        if(e.getrowname.equalsIgnoreCase(rowname)) {
          return rowinfos.length-1-e.getrownumber
        }
      }
      return -1
    }
    def addRequire(Rowname:String,Frequency:Double,Value:String,Requiretype:Int):Unit={
          val rownumber=getRownumber(Rowname)
          if(rownumber >= 0){
            var detal=
            rowinfos(rownumber).addRequire(Frequency,Value,Requiretype)
          }
    }
    def hasTimestamp(): Boolean ={
      for(e <- rowinfos){
        if(e.getrowtype == ROWTYPE.TIMESTAMP)
           return true
      }
      return false
    }
}

class RowRequireList{

  var rowrequires : List[RowRequire]= List()

  def addRequire(Frequency:Double,Value:String,Requiretype:Int): Unit ={
    //插入一条要求
    for(e <- rowrequires) {
      if (e.value == Value){
        e.frequency=math.max(e.frequency,Frequency)
        return
      }
    }
    rowrequires=new RowRequire(Frequency,Value,Requiretype)::rowrequires
  }

  def hasRequire(Value:String):Boolean ={
    for(e <- rowrequires){
      if(e.value == Value)
        return true
    }
    return false;
  }

  def hitRequire(): String ={
    var sum: Double = 0;
    val random = (new Random).nextDouble()
    for (e <- rowrequires) {
      if (e.getrequireType == REQUIRETYPE.INCLUDE) {
        sum += e.frequency
        if (random < sum)
          return e.value
      }
    }
    return null;
  }
  def hitExcept(value:String):Boolean={
    for (e <- rowrequires) {
      if (e.getrequireType == REQUIRETYPE.EXCEPT) {
        if (e.getValue == value)
          return true
      }
    }
    return false
  }
}


class RowRequire(var frequency: Double, var value:String, var requiretype:Int){
  //判断有无和其他的列的相关要求
  def getfFequency=frequency
  def getValue=value
  def getrequireType=requiretype

}




object ROWTYPE{
  val INT=0;
  val STRING=1;
  val FLOAT=2;
  val TIMESTAMP=3;



   def get_rowtype(rowtype:String):Int={

     return rowtype match {//列类型
       case "string" => ROWTYPE.STRING
       case "int" =>ROWTYPE.INT
       case "float"=>ROWTYPE.FLOAT
       case "timestamp"=>ROWTYPE.TIMESTAMP
     }

   }
   def create_rowinfo(rownumber:Int, rowname:String,rowtype:Int): GeneratorRowInfo ={
      return rowtype match {
        case INT => new RowinfoInt(rownumber,rowname)
        case FLOAT => new RowinfoFloat(rownumber,rowname)
        case STRING => new RowinfoString(rownumber,rowname)
        case TIMESTAMP => new RowinfoTimestamp(rownumber,rowname)
      }
   }
}

object  REQUIRETYPE{
  val INCLUDE=0
  val EXCEPT=1
  val FREQUENCY=2

  def getRequiretype(requiretype: String):Int={
    return requiretype match {
      case "include" => REQUIRETYPE.INCLUDE
      case "except" => REQUIRETYPE.EXCEPT
      case "frequency" => REQUIRETYPE.FREQUENCY
      case _ => REQUIRETYPE.INCLUDE
    }
  }
}