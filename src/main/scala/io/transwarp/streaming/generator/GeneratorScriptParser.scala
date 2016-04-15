package io.transwarp.streaming.generator

import java.io.File

import scala.io.Source

/**
  * Created by shentao on 16-2-17.
  */
object GeneratorScriptParser {

  def parse(filename: String): Array[TableInfo] = {

      val file= new File(filename)
      var tables:List[TableInfo]=List()
      var isinTable:Boolean=false
      var isrow:Boolean=false
      var lines = Source.fromFile(file).getLines().toArray
      lines.filter(_.length > 0).foreach(line => {
        val token=line.trim

        if(token.startsWith(Separator.TABLE_START)){ //一开始表示是哪张表
        var tableinfo=token.substring(6).split(":").map(_.trim)
          isinTable=true
          tables=new TableInfo(tableinfo(0),tableinfo(1).toInt)::tables
        }

        if(isinTable){
             if(token.startsWith(Separator.ROWNAME_START)){
              isrow=true
             }
             if(isrow){//列名的处理
                  var rows=token
                 if(token.startsWith(Separator.ROWNAME_START))
                    rows=rows.substring(5)
                 if(token.endsWith(Separator.ROWNAME_END))
                    rows=rows.substring(0,rows.length-1)
                 rows.split(Separator.ROWNAME_SEPARATOR).filter(_.length>0).foreach(tables.head.addRow)
                 if(token.endsWith(Separator.ROWNAME_END)){
                 isrow = false
                }
             }
             if(!isrow && token.indexOf("(")>=0){
                val rows=token.substring( token.indexOf("(")+1, token.indexOf(")") ).split(Separator.ROWNAME_SEPARATOR).filter(_.length>0).map(_.trim)
                val requiretype = REQUIRETYPE.getRequiretype(token.substring( token.indexOf(")")+1, token.indexOf( "(", token.indexOf( ")" )+1 ) ).trim.toLowerCase)
                val value_frequencys=token.substring(token.indexOf("(", token.indexOf(")"))+1, token.indexOf(")", token.indexOf(")")+1)).split(Separator.VALUE_FREQUENCYS_SEPARATOR).filter(_.length>0).map(_.trim)
                for(e <- value_frequencys ){
                      val value_frequency=e.split(Separator.VALUE_FREQUENCYS_ROWS_SEPARATOR).filter(_.length>0).map(_.trim)
                      for(i <- 0 to math.min(rows.length,value_frequency.length)-1){
                        var value_and_frequency=value_frequency(i).split(Separator.VALUE_FREQUENCY_SEPARATOR)
                           if(value_and_frequency.length==2)
                              tables(0).addRequire(rows(i),value_and_frequency(1).toDouble,value_and_frequency(0),requiretype)
                      }
                      }
                }
             }
        if(token.endsWith(Separator.TABLE_END)){
          //退出表
          isinTable=false
        }
      })

    return tables.toArray
  }

}

object Separator{
    val TABLE_START = "table:"
    val TABLE_END = "end;"
    val ROWNAME_START = "row:{"
    val ROWNAME_SEPARATOR = ","
    val ROWNAME_END = "}"
    val VALUE_FREQUENCYS_SEPARATOR = ";"   //各组value和频率的分隔,组与组的分隔
    val VALUE_FREQUENCYS_ROWS_SEPARATOR="," //各列的value和frequency的分隔
    val VALUE_FREQUENCY_SEPARATOR = " "//一组value和频率之间的分隔
}
