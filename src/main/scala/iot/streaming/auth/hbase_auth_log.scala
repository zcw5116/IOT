package iot.streaming.auth

/**
  * Created by slview on 17-6-30.
  */
case class hbase_auth_log(companycode:String, time:String, company_time:String, c_3g_auth_cnt:String,c_3g_success_cnt:String,b_3g_auth_cnt:String,b_3g_success_cnt:String,p_3g_auth_cnt:String,p_3g_success_cnt:String,
                          c_4g_auth_cnt:String,c_4g_success_cnt:String,b_4g_auth_cnt:String,b_4g_success_cnt:String,p_4g_auth_cnt:String,p_4g_success_cnt:String,
                          c_vpdn_auth_cnt:String,c_vpdn_success_cnt:String,b_vpdn_auth_cnt:String,b_vpdn_success_cnt:String,p_vpdn_auth_cnt:String,p_vpdn_success_cnt:String)