package com.oujian.sparkmall.offline.bean

case class CategoryCountInfo (
                             taskInd:String,
                             categoryId:String,
                             orderCount:Long,
                             payCount:Long,
                             clickCount:Long
                             )
