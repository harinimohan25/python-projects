package com.app.csvscrapper
import scala.io.Source
import java.io.{FileNotFoundException, PrintWriter}
import java.lang.StringBuilder

object CsvScrapperApp extends App {
    if (args.length < 3) {
        println("Usage: <inputfile> <outputfile> <no of columns> <optional delimiter>")
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val no_columns = args(2).toInt
    val delimiter = if (args.length == 4) args(3) else ","

    processMultiLine(inputFile, outputFile, no_columns, delimiter)

    def processSingleLine() {
        try{
            val in = Source.fromFile(inputFile)
            val out = new PrintWriter(outputFile)
            (in, out) match {
                case (in, out) => { val ite = in.getLines.foreach( x => out.println(x.split(delimiter).slice(0, no_columns).mkString(delimiter))) }
            }
            in.close()
            out.flush()
            out.close
        }
        catch{
            case ex: FileNotFoundException => println("Could not find input file: " + inputFile)
        }
    }

    def processMultiLine(inputFile: String, outputFile: String, no_columnns: Int, delimiter: String) {
        val in = Source.fromFile(inputFile)
        val out = new PrintWriter(outputFile)

        val ite = in.getLines
        var firstLine = true
        while(ite.hasNext) {
            val input = ite.next
            if (firstLine) {
                val header = input.split(delimiter)
                out.println(input)
                firstLine = false
            } else {
                var line = new StringBuilder(input)
                var cnt = 1
                // Check for un-matched double-quote
                while (ite.hasNext && line.toString.toSeq.count(_ == '\"' ) % 2 == 1) {
                    val line1 = line.toString()
                    val line2 = ite.next
                    // Append consecutive lines within a double-quoted string
                    if (line1.length > 0 && line2.length > 0) {
                        val line1_lastChar = line1.charAt(line1.length-1)
                        val line2_lastChar = line2.charAt(0)
                        if (line1_lastChar != ' ' && line2_firstChar != ' ')
                            line.append(" ")
                }
                line.append(line2)
                cnt += 1
            }
            val tokens = line.toString.replaceAll("\n",",").split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
            out.printn(tokens.slice(0,no_columns).mkString(delimiter))
    }
    }
    in.close()
    out.flush()
    out.close()
   }
}


