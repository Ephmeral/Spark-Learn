package com.ephmeral.bigdata.sparkcore.framework.common

import com.ephmeral.bigdata.sparkcore.framework.util.EnvUtil

trait TDao {
  def readFile(path: String) = {
    EnvUtil.take().textFile(path);
  }
}
