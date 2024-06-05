package com.ephmeral.bigdata.framework.application

import com.ephmeral.bigdata.framework.common.TApplication
import com.ephmeral.bigdata.framework.controller.WordCountController

object WordCountApplication extends App with TApplication {

  // 启动应用程序
  start() {
    val controller = new WordCountController;
    controller.dispatch();
  }
}
