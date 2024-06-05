package com.ephmeral.bigdata.framework.controller

import com.ephmeral.bigdata.framework.common.TController
import com.ephmeral.bigdata.framework.service.WordCountService

class WordCountController extends TController {

  private val wordCountService = new WordCountService;

  // 调度
  def dispatch(): Unit = {
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
