package io.github.zhoujunlin94.connector.mysql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.setting.Setting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

/**
 * @author zhoujunlin
 * @date 2024-08-27-13:42
 */
public class MysqlTableSource extends RichSourceFunction<List<Entity>> {

    private DSFactory dsFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Setting dataSourceTestSetting = new Setting("conf.setting").getSetting("datasource-test");
        this.dsFactory = DSFactory.create(dataSourceTestSetting);
    }

    @Override
    public void run(SourceContext<List<Entity>> sourceContext) throws Exception {
        List<Entity> entityList = Db.use(dsFactory.getDataSource()).findAll(Entity.create("t_order"));
        if (CollUtil.isNotEmpty(entityList)) {
            sourceContext.collect(entityList);
        }
    }

    @Override
    public void cancel() {
        this.dsFactory.close();
    }

}
