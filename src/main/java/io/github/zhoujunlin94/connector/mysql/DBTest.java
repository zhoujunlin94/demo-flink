package io.github.zhoujunlin94.connector.mysql;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.setting.Setting;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

/**
 * @author zhoujunlin
 * @date 2024-08-27-13:19
 */
public class DBTest {

    public static void main(String[] args) throws SQLException {
        Setting dataSourceTestSetting = new Setting("conf.setting").getSetting("datasource-test");
        DataSource dataSourceTest = DSFactory.create(dataSourceTestSetting).getDataSource();
        List<Entity> entityList = Db.use(dataSourceTest).findAll(Entity.create("t_order").set("order_token", "ORDER123"));
        System.out.println(entityList);
    }

}
