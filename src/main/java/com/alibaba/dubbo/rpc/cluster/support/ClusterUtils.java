
package com.alibaba.dubbo.rpc.cluster.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;

import java.util.HashMap;
import java.util.Map;

/**
 * ClusterUtils (修改dubbo的一个bug ，如果服务提供者没有版本号，则reference version=* ,
 * 能获取到服务提供者，但请求到提供方后无法找到对应服务，即对方注册map中没有*为key的提供者
 * 需要删除版本号)
 *
 */
public class ClusterUtils {

    private ClusterUtils() {
    }

    public static URL mergeUrl(URL remoteUrl, Map<String, String> localMap) {
        Map<String, String> map = new HashMap<String, String>();
        Map<String, String> remoteMap = remoteUrl.getParameters();


        if (remoteMap != null && remoteMap.size() > 0) {
            map.putAll(remoteMap);

            // Remove configurations from provider, some items should be affected by provider.
            map.remove(Constants.THREAD_NAME_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.THREAD_NAME_KEY);

            map.remove(Constants.THREADPOOL_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.THREADPOOL_KEY);

            map.remove(Constants.CORE_THREADS_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.CORE_THREADS_KEY);

            map.remove(Constants.THREADS_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.THREADS_KEY);

            map.remove(Constants.QUEUES_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.QUEUES_KEY);

            map.remove(Constants.ALIVE_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.ALIVE_KEY);

            map.remove(Constants.TRANSPORTER_KEY);
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.TRANSPORTER_KEY);
        }

        if (localMap != null && localMap.size() > 0) {
        	//version * bug
        	if("*".equals(Constants.VERSION_KEY)) {
        		localMap.remove(Constants.VERSION_KEY);
        	}
            map.putAll(localMap);
        }
        if (remoteMap != null && remoteMap.size() > 0) {
            // Use version passed from provider side
            String dubbo = remoteMap.get(Constants.DUBBO_VERSION_KEY);
            if (dubbo != null && dubbo.length() > 0) {
                map.put(Constants.DUBBO_VERSION_KEY, dubbo);
            }
            String version = remoteMap.get(Constants.VERSION_KEY);
            if (version != null && version.length() > 0) {
                map.put(Constants.VERSION_KEY, version);
            }
            String group = remoteMap.get(Constants.GROUP_KEY);
            if (group != null && group.length() > 0) {
                map.put(Constants.GROUP_KEY, group);
            }
            String methods = remoteMap.get(Constants.METHODS_KEY);
            if (methods != null && methods.length() > 0) {
                map.put(Constants.METHODS_KEY, methods);
            }
            // Reserve timestamp of provider url.
            String remoteTimestamp = remoteMap.get(Constants.TIMESTAMP_KEY);
            if (remoteTimestamp != null && remoteTimestamp.length() > 0) {
                map.put(Constants.REMOTE_TIMESTAMP_KEY, remoteMap.get(Constants.TIMESTAMP_KEY));
            }
            // Combine filters and listeners on Provider and Consumer
            String remoteFilter = remoteMap.get(Constants.REFERENCE_FILTER_KEY);
            String localFilter = localMap.get(Constants.REFERENCE_FILTER_KEY);
            if (remoteFilter != null && remoteFilter.length() > 0
                    && localFilter != null && localFilter.length() > 0) {
                localMap.put(Constants.REFERENCE_FILTER_KEY, remoteFilter + "," + localFilter);
            }
            String remoteListener = remoteMap.get(Constants.INVOKER_LISTENER_KEY);
            String localListener = localMap.get(Constants.INVOKER_LISTENER_KEY);
            if (remoteListener != null && remoteListener.length() > 0
                    && localListener != null && localListener.length() > 0) {
                localMap.put(Constants.INVOKER_LISTENER_KEY, remoteListener + "," + localListener);
            }
        }

        return remoteUrl.clearParameters().addParameters(map);
    }

}