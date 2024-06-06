package com.ephmeral.bigdata.sparkcore.framework.controller

import com.ephmeral.bigdata.sparkcore.framework.common.TController
import com.ephmeral.bigdata.sparkcore.framework.service.WordCountService

class WordCountController extends TController {

  private val wordCountService = new WordCountService;

  // 调度
  def dispatch(): Unit = {
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
