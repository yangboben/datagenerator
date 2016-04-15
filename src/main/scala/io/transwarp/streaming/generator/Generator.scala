package io.transwarp.streaming.generator

import java.io.{File, PrintWriter}

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLastValue

import scala.util.Random

/**
  * Created by shentao on 16-2-25.
  */
object Generator {

    val partation_number=4

    def generate(tableinfos :Array[TableInfo]):Unit={
         for(e <- tableinfos)
            generate_table(e)

    }
    def generate_table(tableinfo:TableInfo):Unit={

        val writer = new PrintWriter(new File(tableinfo.tablename))
        var partation=0

       for(e <- 1 to tableinfo.linenumber){

          val line = generate_line(tableinfo)
          writer.println(line)
          //LoadKafka.LoadDatatoKafka(tableinfo.tablename, line, GeneratorRunner.producer, partation)
          //partation=(partation+1)%partation_number
        }
        writer.close()
    }

    def generate_line(tableinfo:TableInfo):String={
        var line = ""

        for(e <- 1 to tableinfo.rowinfos.length){
          line= line + generate_row(tableinfo.rowinfos(tableinfo.rowinfos.length-e))+","
        }
        line=line.substring(0,line.length-1)
        return line
    }

    def generate_row(rowinfo: GeneratorRowInfo):String={

            val result = rowinfo.generate_row()

      return  result
    }

}
