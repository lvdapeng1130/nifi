package org.apache.nifi.controller.cluster;

import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;

public class DefaultZookeeperFactory implements ZookeeperFactory
{
    private ZKClientConfig zkSecureClientConfig;
    public DefaultZookeeperFactory(final ZooKeeperClientConfig zkConfig){
        this.zkSecureClientConfig = new ZKClientConfig();
        zkSecureClientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER, Integer.toString(zkConfig.getJuteMaxbuffer()));
        zkSecureClientConfig.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, String.valueOf(zkConfig.isEnableClientSasl()));
    }
    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception
    {
        return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly,zkSecureClientConfig);
    }
}