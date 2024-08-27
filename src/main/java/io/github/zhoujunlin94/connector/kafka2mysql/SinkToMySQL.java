package io.github.zhoujunlin94.connector.kafka2mysql;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.DSFactory;
import com.alibaba.fastjson.JSONObject;
import io.github.zhoujunlin94.common.DSFactoryCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


/**
 * @author zhoujunlin
 * @date 2024-08-27-14:12
 */
public class SinkToMySQL extends RichSinkFunction<JSONObject> {

    private DSFactory dsFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dsFactory = DSFactoryCache.get("datasource-test");
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        Entity entity = Entity.create(value.getString("table_name"))
                .set("user_id", value.getInteger("user_id")).set("order_token", value.getString("order_token"));
        Db.use(this.dsFactory.getDataSource()).insert(entity);
    }

}
