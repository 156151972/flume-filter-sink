package com.sunmya.flume.sinks.filter;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 自定义Sink 根据第N个字段进行过滤，过滤后的数据传输到avro接口
 * 
 * @author sun.mengya
 * <br>
 * 
 *         a1.sinks.k1.type=com.theta.flume.sinks.filter.FilterAvroSink<br>
 *         a1.sinks.k1.hostname=slave5 avro ip <br>
 *         a1.sinks.k1.port=55555 avro端口<br>
 *         a1.sinks.k1.batchsize=10000 每次最多从channel中拿多少数据<br>
 *         a1.sinks.k1.field.separator = \\| 分割符<br>
 *         a1.sinks.k1.conditions = 2$<=$20 条件<br>
 *
 */
public class FilterAvroSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(FilterAvroSink.class);
	// avro host sink处理后向该host发数据
	private String hostname;
	// avro port
	private Integer port;

	private RpcClient client;
	private Properties clientProps;
	private SinkCounter sinkCounter;
	private int cxnResetInterval;
	private AtomicBoolean resetConnectionFlag;
	private final int DEFAULT_CXN_RESET_INTERVAL = 0;
	private final ScheduledExecutorService cxnResetExecutor = Executors
			.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Rpc Sink Reset Thread").build());

	// 字段分割符 a1.sinks.k1.field.separator = \\|(|要用\\转义)
	private String separator;
	// 条件 a1.sinks.k1.conditions = 2$<=$20
	private String conditions;

	/*
	 * 加载配置，从flume-ng 启动时指定的配置文件中加载配置的: agent name.sinks.sink name.xxx
	 * 
	 * @see
	 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		clientProps = new Properties();

		hostname = context.getString("hostname");
		port = context.getInteger("port");
		separator = context.getString("field.separator", "|");
		conditions = context.getString("conditions");

		Preconditions.checkState(hostname != null, "No hostname specified");
		Preconditions.checkState(port != null, "No port specified");

		clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
		clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1", hostname + ":" + port);

		for (Entry<String, String> entry : context.getParameters().entrySet()) {
			clientProps.setProperty(entry.getKey(), entry.getValue());
		}

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
		cxnResetInterval = context.getInteger("reset-connection-interval", DEFAULT_CXN_RESET_INTERVAL);
		if (cxnResetInterval == DEFAULT_CXN_RESET_INTERVAL) {
			logger.info("Connection reset is set to " + String.valueOf(DEFAULT_CXN_RESET_INTERVAL)
					+ ". Will not reset connection to next " + "hop");
		}
	}

	/**
	 * Returns a new {@linkplain RpcClient} instance configured using the given
	 * {@linkplain Properties} object. This method is called whenever a new
	 * connection needs to be created to the next hop.
	 * 
	 * @param props
	 * @return
	 */

	/**
	 * If this function is called successively without calling {@see
	 * #destroyConnection()}, only the first call has any effect.
	 * 
	 * @throws org.apache.flume.FlumeException
	 *             if an RPC client connection could not be opened
	 */
	private void createConnection() throws FlumeException {

		if (client == null) {
			logger.info("Rpc sink {}: Building RpcClient with hostname: {}, " + "port: {}", new Object[] { getName(),
					hostname, port });
			try {
				resetConnectionFlag = new AtomicBoolean(false);
				client = initializeRpcClient(clientProps);
				Preconditions.checkNotNull(client, "Rpc Client could not be " + "initialized. " + getName()
						+ " could not be started");
				sinkCounter.incrementConnectionCreatedCount();
				if (cxnResetInterval > 0) {
					cxnResetExecutor.schedule(new Runnable() {
						@Override
						public void run() {
							resetConnectionFlag.set(true);
						}
					}, cxnResetInterval, TimeUnit.SECONDS);
				}
			} catch (Exception ex) {
				sinkCounter.incrementConnectionFailedCount();
				if (ex instanceof FlumeException) {
					throw (FlumeException) ex;
				} else {
					throw new FlumeException(ex);
				}
			}
			logger.debug("Rpc sink {}: Created RpcClient: {}", getName(), client);
		}

	}

	private void resetConnection() {
		try {
			destroyConnection();
			createConnection();
		} catch (Throwable throwable) {
			// Don't rethrow, else this runnable won't get scheduled again.
			logger.error("Error while trying to expire connection", throwable);
		}
	}

	private void destroyConnection() {
		if (client != null) {
			logger.debug("Rpc sink {} closing Rpc client: {}", getName(), client);
			try {
				client.close();
				sinkCounter.incrementConnectionClosedCount();
			} catch (FlumeException e) {
				sinkCounter.incrementConnectionFailedCount();
				logger.error("Rpc sink " + getName() + ": Attempt to close Rpc " + "client failed. Exception follows.",
						e);
			}
		}

		client = null;
	}

	/**
	 * Ensure the connection exists and is active. If the connection is not
	 * active, destroy it and recreate it.
	 *
	 * @throws org.apache.flume.FlumeException
	 *             If there are errors closing or opening the RPC connection.
	 */
	private void verifyConnection() throws FlumeException {
		if (client == null) {
			createConnection();
		} else if (!client.isActive()) {
			destroyConnection();
			createConnection();
		}
	}

	/**
	 * The start() of RpcSink is more of an optimization that allows connection
	 * to be created before the process() loop is started. In case it so happens
	 * that the start failed, the process() loop will itself attempt to
	 * reconnect as necessary. This is the expected behavior since it is
	 * possible that the downstream source becomes unavailable in the middle of
	 * the process loop and the sink will have to retry the connection again.
	 */
	@Override
	public void start() {
		logger.info("Starting {}...**********************", this);
		sinkCounter.start();
		try {
			createConnection();
		} catch (FlumeException e) {
			logger.warn("Unable to create Rpc client using hostname: " + hostname + ", port: " + port, e);

			/* Try to prevent leaking resources. */
			destroyConnection();
		}

		super.start();

		logger.info("Filter Avro {} started.", getName());
	}

	@Override
	public void stop() {
		logger.info("Filter Avro {} stopping...", getName());
		destroyConnection();
		cxnResetExecutor.shutdown();
		try {
			if (cxnResetExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
				cxnResetExecutor.shutdownNow();
			}
		} catch (Exception ex) {
			logger.error("Interrupted while waiting for connection reset executor " + "to shut down");
		}
		sinkCounter.stop();
		super.stop();

		logger.info("Filter Avro {} stopped. Metrics: {}", getName(), sinkCounter);
	}

	/**
	 * 执行的方�?
	 */
	@Override
	public Status process() throws EventDeliveryException {
		boolean getEvent = false;
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		if (resetConnectionFlag.get()) {
			resetConnection();
			// if the time to reset is long and the timeout is short
			// this may cancel the next reset request
			// this should however not be an issue
			resetConnectionFlag.set(false);
		}
		logger.info("#########    Begining get event   #########");
		try {
			transaction.begin();

			verifyConnection();

			List<Event> batch = Lists.newLinkedList();

			// 配置文件中指定batchsize，默认为100
			int batchSize = client.getBatchSize();

			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();

				if (event == null) {
					break;
				}
				getEvent = true;
				if (!checkValue(event)) {
					logger.debug("false");
					// 之前使用break导致当不符合条件时就会跳出，然后重新建立线程从channel中取数据，效率低下
					// 改为continue 之后每次从channel里拿出min(batchsize,event num)
					// 条数据，效率获得提升
					continue;
				}

				batch.add(event);
			}

			int size = batch.size();

			// 如果不判断getEvent
			// 的值，在没有符合条件的数据时程序会认为channel中没有数据，此时程序会休眠，每次休眠时间增加1s,最长休眠时间为5s
			if (size == 0 && !getEvent) {
				sinkCounter.incrementBatchEmptyCount();
				// 返回 Status.BACKOFF 时休眠后重新获取数据
				status = Status.BACKOFF;
			} else {
				logger.info("#########  get event: " + size + "  #########");
				if (size < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(size);
				client.appendBatch(batch);
			}

			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(size);

		} catch (Throwable t) {
			transaction.rollback();
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof ChannelException) {
				logger.error("Rpc Sink " + getName() + ": Unable to get event from" + " channel " + channel.getName()
						+ ". Exception follows.", t);
				status = Status.BACKOFF;
			} else {
				destroyConnection();
				throw new EventDeliveryException("Failed to send events", t);
			}
		} finally {
			getEvent = false;
			transaction.close();
		}
		logger.info("#########    End get event  #########");
		return status;
	}

	@VisibleForTesting
	RpcClient getUnderlyingClient() {
		return client;
	}

	protected RpcClient initializeRpcClient(Properties props) {
		logger.info("Attempting to create Avro Rpc client.");
		return RpcClientFactory.getInstance(props);
	}

	@Override
	public String toString() {
		return "RpcSink " + getName() + " { host: " + hostname + ", port: " + port + " }";
	}

	public boolean checkValue(Event event) {

		// 1.将事件中的内容转换为字符串
		byte[] body = event.getBody();
		String line = new String(body).trim();
		// 没有条件时所有数据都符合条件
		if (conditions == null) {
			return true;
		}

		/*
		 * 原计划支持多条件，暂时只支持单条件过滤 String[] conditionArray ;
		 * 
		 * if(conditions.indexOf("||")>0){ conditionArray =
		 * conditions.split("||"); }else if(conditions.indexOf("&&")>0){
		 * conditionArray = conditions.split("&&"); }else{ conditionArray = new
		 * String[1]; conditionArray[0] = conditions; }
		 */

		// 条件中的运算符使用"$" 分割 条件 2$<=$100 ，意思为下标为2的数据小于等于 100
		String[] split = conditions.split("\\$");

		// 如果条件不合法，默认所有数据都不满足条件
		if (split.length != 3) {
			return false;
		}

		Integer index = Integer.parseInt(split[0]);
		String operator = split[1];
		String condition = split[2];
		String[] fieldArray;
		fieldArray = line.split(separator);

		if (index > fieldArray.length) {
			return false;
		}
		boolean check = false;
		long conditionValue = Long.parseLong(condition);
		switch (operator) {
		case ">":
			check = Integer.parseInt(fieldArray[index]) > conditionValue;
			break;
		case ">=":
			check = Integer.parseInt(fieldArray[index]) >= conditionValue;
			break;
		case "<":
			check = Integer.parseInt(fieldArray[index]) < conditionValue;
			break;
		case "<=":
			check = Integer.parseInt(fieldArray[index]) <= conditionValue;
			break;
		case "<>":
		case "><":
		case "!=":
			check = Integer.parseInt(fieldArray[index]) != conditionValue;
			break;
		case "=":
		case "==":
			check = Integer.parseInt(fieldArray[index]) == conditionValue;
			break;
		default:
			break;
		}
		// logger.debug(fieldArray[index] + "***" + conditionValue);

		return check;
	}
}
