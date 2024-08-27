package io.github.zhoujunlin94.common;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.ds.DSFactory;
import cn.hutool.setting.Setting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhoujunlin
 * @date 2024-08-27-14:48
 */
public class DSFactoryCache {

    private static final Map<String, DSFactory> DS_FACTORY_CACHE = new ConcurrentHashMap<>();
    private static final String SUFFIX = "-ds-factory";

    static {
        init(SettingFactory.CONF_SETTING, "datasource-test");
    }

    private static void init(Setting setting, String group) {
        Setting dataSourceTestSetting = setting.getSetting(group);
        DSFactory dsFactory = DSFactory.create(dataSourceTestSetting);
        DS_FACTORY_CACHE.put(makeKey(group), dsFactory);
    }

    private static String makeKey(String group) {
        return StrUtil.addSuffixIfNot(group, SUFFIX);
    }

    public static DSFactory get(String group) {
        return DS_FACTORY_CACHE.get(makeKey(group));
    }

}
