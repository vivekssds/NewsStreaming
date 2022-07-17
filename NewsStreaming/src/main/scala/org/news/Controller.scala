package org.news

import com.typesafe.scalalogging.LazyLogging
import org.news.enums._
import org.news.processors.NewsProcessor

class Controller(config: ApplicationConfig) extends LazyLogging with InitSpark  {

  private val eventAction = EventAction(config.eventAction)
  println(eventAction)

  def start(): Unit = {
    try {
      eventAction match {
        case EventAction.newsstreaming =>
          val NewsProcessor = new NewsProcessor(config)
          NewsProcessor.start()
      }
    }
    catch{
      case e: Exception =>
        e.printStackTrace()
        logger.error(e.getMessage)
        throw e
    }
  }

}
