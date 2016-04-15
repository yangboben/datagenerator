package io.transwarp.streaming.generator

import java.lang.Exception

import scala.util.Random

/**
  * Created by shentao on 16-3-30.
  */
abstract class GeneratorRowInfo(val rownumber:Int, val rowname:String){
  //一列的生成信息
  //该列在list中的真正下标为list.length-1-rownumber
  val rowtype:Int
  def getrownumber=rownumber
  def getrowname=rowname
  def getrowtype=rowtype


  var require :RowRequireList = new RowRequireList()
  var distribution:distribution = new distribution_normal()

  def addRequire(Frequency:Double,Value:String,Requiretype:Int): Unit ={
    require.addRequire(Frequency,Value,Requiretype)
  }

  def setdistribution(distribution:distribution): Unit ={
      this.distribution=distribution
  }
  def generate_row():String
}

class RowinfoInt(override val rownumber:Int,override val rowname:String)extends GeneratorRowInfo(rownumber,rowname){

   override val rowtype = ROWTYPE.INT

    def generate_row():String= {

      var result = require.hitRequire()
      if(result!=null)
          return result

      var value: Int = 0
      while (true){
        value = distribution.get_next_value.toInt
        if(!require.hitExcept(value+"")) return value.toString
      }
     return ""
    }

}

class RowinfoFloat(override val rownumber:Int,override val rowname:String)extends GeneratorRowInfo(rownumber,rowname){
  override val rowtype: Int = ROWTYPE.FLOAT

  override def generate_row(): String = {
     var result = require.hitRequire()
     if(result!=null)
         return result

     var value:Double =0.0
    while(true){
      value = distribution.get_next_value
      if(!require.hitExcept(value+"")) return value.toString
    }
    return ""
  }
}

class RowinfoString(override val rownumber:Int,override val rowname:String)extends GeneratorRowInfo(rownumber,rowname){

  override val rowtype: Int = ROWTYPE.STRING

  override def generate_row(): String = {
    //todo generate string
     return "default"
  }


}

class RowinfoTimestamp(override val rownumber:Int,override val rowname:String)extends GeneratorRowInfo(rownumber,rowname){
  override val rowtype: Int = ROWTYPE.TIMESTAMP
  distribution = new distribution_uniform()
  var timestamp = new timestamp(1970,1,1,7,0,0);

  override def generate_row(): String = {
    var result = require.hitRequire()
    if (result != null)
      return result

     while(true) {
      var value=timestamp.add(distribution.get_next_value.toInt)
       if(!require.hitExcept(value.toString())) return value.toString()
     }
    return ""
  }

  class timestamp(var year:Int, var month:Int, var day:Int,
             var hour:Int, var minute:Int, var second:Int,timestamp: String) {


    if(timestamp != null){
      val infos= timestamp.split("[ :-]")
      year = infos(0).toInt
      month = infos(1).toInt
      day = infos(2).toInt
      hour = infos(3).toInt
      minute = infos(4).toInt
      second = infos(5).toInt
    }

    def this(year:Int, month:Int,  day:Int,
             hour:Int, minute:Int, second:Int)=this(year:Int, month:Int, day:Int,
                                                    hour:Int, minute:Int,second:Int,null)

    def this(timestamp: String) = this(0,0,0,0,0,0,timestamp)


    def add(delta: Int): timestamp = {

      var second = this.second+delta
      var minute = this.minute+second / 60
          second %= 60
      var hour   = this.hour+minute / 60
          minute %= 60
      var day    = this.day+hour / 24
          hour   %= 24
      var month  = this.month+day / 30
          day    %= 30
      var year   = this.year+month / 12

      return new timestamp(year,month,day,hour,minute,second)
    }

    override def toString():String = {
      return year+"-"+add_zero(month)+"-"+add_zero(day)+" "+
             add_zero(hour)+":"+add_zero(minute)+":"+add_zero(second)
    }

    def add_zero(number:Int):String={
        return if(number<10) "0"+number
               else  number+""
    }
  }
}