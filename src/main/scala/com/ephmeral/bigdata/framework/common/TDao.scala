package com.ephmeral.bigdata.framework.common

import com.ephmeral.bigdata.framework.util.EnvUtil

trait TDao {
  def readFile(path: String) = {
    EnvUtil.take().textFile(path);
  }
}
