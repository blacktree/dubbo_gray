package lz.dubbo.gray;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;
import com.alibaba.dubbo.rpc.cluster.support.AbstractClusterInvoker;

public class GrayClusterInvoker<T> extends AbstractClusterInvoker<T> {

	private static final String Gray_flag="gray";

	private final Log logger=LogFactory.getLog(this.getClass());


	public GrayClusterInvoker(Directory<T> directory) {
		super(directory);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
			throws RpcException {

		List<Invoker<T>> copyinvokers = getInvokers(invokers);
		checkInvokers(copyinvokers, invocation);
		int len = getUrl().getMethodParameter(invocation.getMethodName(), Constants.RETRIES_KEY, Constants.DEFAULT_RETRIES) + 1;
		if (len <= 0) {
			len = 1;
		}
		// retry loop.
		RpcException le = null; // last exception.
		List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyinvokers.size()); // invoked invokers.
		Set<String> providers = new HashSet<String>(len);
		for (int i = 0; i < len; i++) {
			//Reselect before retry to avoid a change of candidate `invokers`.
			//NOTE: if `invokers` changed, then `invoked` also lose accuracy.
			if (i > 0) {
				checkWhetherDestroyed();
				copyinvokers = list(invocation);
				// check again
				checkInvokers(copyinvokers, invocation);
			}
			Invoker<T> invoker = select(loadbalance, invocation, copyinvokers, invoked);
			invoked.add(invoker);
			RpcContext.getContext().setInvokers((List) invoked);
			try {
				Result result = invoker.invoke(invocation);
				if (le != null && logger.isWarnEnabled()) {
					logger.warn("Although retry the method " + invocation.getMethodName()
					+ " in the service " + getInterface().getName()
					+ " was successful by the provider " + invoker.getUrl().getAddress()
					+ ", but there have been failed providers " + providers
					+ " (" + providers.size() + "/" + copyinvokers.size()
					+ ") from the registry " + directory.getUrl().getAddress()
					+ " on the consumer " + NetUtils.getLocalHost()
					+ " using the dubbo version " + Version.getVersion() + ". Last error is: "
					+ le.getMessage(), le);
				}
				return result;
			} catch (RpcException e) {
				if (e.isBiz()) { // biz exception.
					throw e;
				}
				le = e;
			} catch (Throwable e) {
				le = new RpcException(e.getMessage(), e);
			} finally {
				providers.add(invoker.getUrl().getAddress());
			}
		}
		throw new RpcException(le != null ? le.getCode() : 0, "Failed to invoke the method "
				+ invocation.getMethodName() + " in the service " + getInterface().getName()
				+ ". Tried " + len + " times of the providers " + providers
				+ " (" + providers.size() + "/" + copyinvokers.size()
				+ ") from the registry " + directory.getUrl().getAddress()
				+ " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
				+ Version.getVersion() + ". Last error is: "
				+ (le != null ? le.getMessage() : ""), le != null && le.getCause() != null ? le.getCause() : le);

	}


	private List<Invoker<T>> getInvokers(List<Invoker<T>> invokers) {

		List<Invoker<T>> target=new ArrayList<Invoker<T>>();

		for(Invoker<T> inv:invokers) {
			String version=inv.getUrl().getParameter(Constants.VERSION_KEY);
			if(version!=null && version.equals(Gray_flag)) {
				target.add(inv);
			}
		}

		//没有灰度服务，走正常版本服务
		if(target.size()==0)
			target=invokers;
		return target;
	}

}
