package lz.dubbo.gray;

import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Directory;

public class GrayCluster implements Cluster {

	@Override
	public <T> Invoker<T> join(Directory<T> directory) throws RpcException {
		// TODO Auto-generated method stub
		return new GrayClusterInvoker<T>(directory);
	}

}
