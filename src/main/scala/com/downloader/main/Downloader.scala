package com.downloader.main

import java.awt.image.RenderedImage
import java.io.File

import javax.imageio.ImageIO
import java.net.{HttpURLConnection, URL}

import scala.collection.mutable.ArrayBuffer

object Downloader extends App {

  val usage = """
    Usage: downloader --input-csv filename --dest-dir dirname
  """

  def fileDownload(urlString: String, filename: String) = {
    try {
      val url = new URL(urlString)
      val image : RenderedImage = ImageIO.read(url)
      ImageIO.write(image,"jpg", new File(filename))
    } catch {
      case e: java.io.IOException => "Images not found"
    }
  }

  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
      try {
        f(resource)
      } finally {
        resource.close()
  }

  def getImageIdsFromFile(filePath: String = "human_20k_annotatation_with_classnames.csv"): Array[String] = {
    val rows = ArrayBuffer[Array[String]]()
    using(io.Source.fromFile(filePath)) { source =>
      for (line <- source.getLines) {
        rows += line.split(",").map(_.trim)
      }
    }

    //rows.slice(0, 5).foreach {row => println(s"${row(1)}")}
    rows.slice(1, rows.size-1).map {a => a(1)}(collection.breakOut)
  }

  override def main(args: Array[String]): Unit = {

    if (args.length < 2) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]


    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = s(0) == '-'

      list match {
        case Nil => map
        case "--input-csv" :: value :: tail => nextOption(map ++ Map('inputcsv -> value.toString), tail)
        case "--dest-dir" :: value :: tail => nextOption(map ++ Map('destdir -> value.toString), tail)
        case option :: tail => println(s"Unknown option $option")
                                sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)
    println(options)

    val destdir = options(Symbol("destdir")).toString
    val inputCsv = options(Symbol("inputcsv")).toString


    val bucketName = "open-images-dataset"
    val imageIds = getImageIdsFromFile(inputCsv)
    imageIds.slice(0, 2).foreach {println}
    val baseUrl = s"http://$bucketName.s3.amazonaws.com/train/"
    val urls = imageIds.par.map { id =>  baseUrl + id + ".jpg" }
    urls.slice(0, 2).foreach { println }
    val filenames = imageIds.map { id =>  s"$destdir\\$id.jpg"}
    urls.zip(filenames).par.foreach { case (url, filename) => fileDownload(urlString = url, filename = filename) }

  }

}
