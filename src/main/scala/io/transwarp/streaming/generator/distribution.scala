package io.transwarp.streaming.generator

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLastValue

import scala.util.Random

/**
  * Created by shentao on 16-3-30.
  */

object DISTRIBUTION{

  val UNIFORM =0 //均匀分布
  val NORMAL  =1//正态分布

}

abstract class distribution{
  val distribution_type: Int
  def get_next_value:Double
}

class distribution_normal(var range:Double = 10) extends distribution(){

   override val distribution_type: Int = DISTRIBUTION.NORMAL

   override def get_next_value: Double = {

    var result = ((Random.nextGaussian()*range/3) + (range/2))

    if(result>range)
      result =range
    if(result<0)
      result=0

    return  result;
  }

  def set_range(range:Double): Unit ={
    this.range=range
  }


}

class distribution_uniform(var lastValue: Double =0.0,var frequency:Int =10,var delta:Double=1.0,var origin:Double=0)extends distribution(){
  override val distribution_type =DISTRIBUTION.UNIFORM
  var offset=0;
  override def get_next_value: Double = {
     lastValue = lastValue+(offset/frequency)*delta
     offset=(offset%frequency)+1;
    return lastValue
  }

  def set_argument(lastValue: Double,frequency:Int,delta:Double): Unit ={
      this.lastValue=lastValue
      this.frequency=frequency
      this.delta=delta
  }
}
