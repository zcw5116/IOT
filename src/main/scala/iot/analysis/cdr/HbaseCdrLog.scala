package iot.analysis.cdr

/**
  * Created by slview on 17-7-2.
  */

case class HbaseCdrLog(companycode:String, time:String, company_time:String,
                       c_haccg_upflow:String,c_haccg_downflow:String,c_pgw_upflow:String,c_pgw_downflow:String,
                       p_haccg_upflow:String,p_haccg_downflow:String,p_pgw_upflow:String,p_pgw_downflow:String,
                       b_haccg_upflow:String,b_haccg_downflow:String,b_pgw_upflow:String,b_pgw_downflow:String)

