package com.dmp.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dmp.bean.BusinessAreas;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by angel
 */
public class ParseJson {
    public static String parse(String json){
        /*
        <regeocode>
            <addressComponent>
                <businessAreas type="list">
                    <businessArea>
                        <location>116.31060892521111,39.99231773703259</location>
                        <name>北京大学</name>
                        <id>110108</id>
                    </businessArea>
                    ....
                    ....
                </businessAreas>
            </addressComponent>
        </regeocode>
        * */
        JSONObject jsonObject = JSON.parseObject(json);
        JSONObject regeocode = (JSONObject)jsonObject.get("regeocode");
        JSONObject addressComponent = (JSONObject)regeocode.get("addressComponent");
        JSONArray businessAreas = addressComponent.getJSONArray("businessAreas");

        List<BusinessAreas> list = JSON.parseArray(businessAreas.toJSONString(), BusinessAreas.class);
        StringBuffer sb = new StringBuffer();
        for(int i=0 ; i<list.size()-1 ; i++ ){
            if(list.get(i).getName() != null && StringUtils.isNotBlank(list.get(i).getName())){
                sb.append(list.get(i).getName()).append(":");//软件园:上地:
            }
        }
        if(StringUtils.isNotBlank(sb.toString())){
            String data = sb.toString();
            return data.substring(0 , data.length()-1);//软件园:上地
        }else{
            return "blank";
        }

    }
}
